package xyz.kafka.connector.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.redisson.api.RLock;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.config.RedissonConfig;
import xyz.kafka.connector.enums.RedisClientType;
import xyz.kafka.connector.pauser.PartitionPauser;
import xyz.kafka.utils.StringUtil;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * RateLimitSinkTask
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-03-23
 */
public abstract class RateLimitSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(RateLimitSinkTask.class);
    public static final String RATE_LIMITER_KEY = "rate.limiter.key";
    public static final String RATE_LIMITER_SIZE = "rate.limiter.size";
    public static final String RATE_LIMITER_INTERVAL_MS = "rate.limiter.interval.ms";
    public static final String RATE_LIMITER_ACQUIRE_MS = "rate.limiter.acquire.ms";
    public static final String BATCH_SIZE = "batch.size";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    RATE_LIMITER_KEY,
                    ConfigDef.Type.STRING,
                    "rate:limiter",
                    ConfigDef.Importance.MEDIUM,
                    "Rate Limiter Key."
            ).define(
                    RATE_LIMITER_SIZE,
                    ConfigDef.Type.LONG,
                    StringUtil.toLong(System.getenv("RATE_LIMITER_SIZE"), 1000000L),
                    ConfigDef.Importance.MEDIUM,
                    "Rate Limiter Size."
            ).define(
                    RATE_LIMITER_INTERVAL_MS,
                    ConfigDef.Type.LONG,
                    StringUtil.toLong(System.getenv("RATE_LIMITER_INTERVAL_MS"), 1000L),
                    ConfigDef.Importance.MEDIUM,
                    "Rate Limiter Interval Ms"
            ).define(
                    RATE_LIMITER_ACQUIRE_MS,
                    ConfigDef.Type.LONG,
                    StringUtil.toLong(System.getenv("RATE_LIMITER_ACQUIRE_MS"), 1000L),
                    ConfigDef.Importance.MEDIUM,
                    "Rate Limiter Interval Ms"
            ).define(
                    BATCH_SIZE,
                    ConfigDef.Type.INT,
                    2000,
                    ConfigDef.Importance.MEDIUM,
                    "Batch size"
            );

    private long limit;
    private long intervalMs;
    private String filterKey;
    private String lockKey;
    private PartitionPauser partitionPauser;

    protected RedissonClient redisson;
    protected RedisClientType redisClientType;
    protected RRateLimiter limiter;
    protected int batchSize;
    @Override
    public void start(Map<String, String> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.limit = config.getLong(RATE_LIMITER_SIZE);
        this.intervalMs = config.getLong(RATE_LIMITER_INTERVAL_MS);
        long acquireMs = config.getLong(RATE_LIMITER_ACQUIRE_MS);
        RedissonConfig redissonConfig = new RedissonConfig(props);
        this.redisson = redissonConfig.getRedisson();
        this.redisClientType = redissonConfig.getRedisClientType();
        this.filterKey = config.getString(RATE_LIMITER_KEY);
        this.batchSize = config.getInt(BATCH_SIZE);
        this.lockKey = "distributed_lock:" + filterKey;
        init();
        this.partitionPauser = new PartitionPauser(
                context,
                () -> !this.limiter.tryAcquire(batchSize, Duration.ofMillis(acquireMs)),
                () -> this.limiter.tryAcquire(batchSize, Duration.ofMillis(acquireMs))
        );
    }

    private void init() {
        this.limiter = redisson.getRateLimiter(this.filterKey);
        if (this.limiter.isExists()) {
            return;
        }
        RLock lock = this.redisson.getLock(this.lockKey);
        try {
            lock.lock(10000, TimeUnit.MILLISECONDS);
            if (this.limiter.isExists()) {
                return;
            }
            this.limiter.trySetRate(RateType.OVERALL, limit, Duration.ofMillis(intervalMs));
            log.info("inited rate limiter:{}", this.filterKey);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        partitionPauser.maybeResumePartitions();
        doPut(records);
        partitionPauser.maybePausePartitions();
    }

    /**
     * put data
     *
     * @param records
     */
    public abstract void doPut(Collection<SinkRecord> records);
}
