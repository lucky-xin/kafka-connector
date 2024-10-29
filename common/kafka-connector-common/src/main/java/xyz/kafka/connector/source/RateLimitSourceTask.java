package xyz.kafka.connector.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.redisson.api.RLock;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.config.RedissonConfig;
import xyz.kafka.connector.enums.RedisClientType;
import xyz.kafka.utils.StringUtil;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * RateLimitSourceTask
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-03-23
 */
public abstract class RateLimitSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(RateLimitSourceTask.class);
    private static final String RATE_LIMITER_KEY = "rate.limiter.key";
    private static final String RATE_LIMITER_SIZE = "rate.limiter.size";
    private static final String RATE_LIMITER_INTERVAL_MS = "rate.limiter.interval.ms";
    private static final String RATE_LIMITER_ACQUIRE_MS = "rate.limiter.acquire.ms";
    private static final String BATCH_SIZE = "batch.size";

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
                    StringUtil.toLong(System.getenv("RATE_LIMITER_SIZE"), 10000L),
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

    private long acquireMs;

    protected RedissonClient redisson;
    protected RedisClientType redisClientType;
    protected RRateLimiter limiter;
    protected int batchSize;

    @Override
    public void start(Map<String, String> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.limit = config.getLong(RATE_LIMITER_SIZE);
        this.intervalMs = config.getLong(RATE_LIMITER_INTERVAL_MS);
        RedissonConfig redissonConfig = new RedissonConfig(props);
        this.redisson = redissonConfig.getRedisson();
        this.redisClientType = redissonConfig.getRedisClientType();
        this.filterKey = config.getString(RATE_LIMITER_KEY);
        this.lockKey = "distributed_lock:" + filterKey;
        this.acquireMs = config.getLong(RATE_LIMITER_ACQUIRE_MS);
        this.batchSize = config.getInt(BATCH_SIZE);
        init();
    }

    private void init() {
        this.limiter = redisson.getRateLimiter(this.filterKey);
        if (this.limiter.isExists()) {
            return;
        }
        RLock lock = this.redisson.getLock(this.lockKey);
        long waiteTime = 10000;
        try {
            lock.lock(waiteTime, TimeUnit.MILLISECONDS);
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
    public List<SourceRecord> poll() {
        if (!this.limiter.tryAcquire(batchSize, Duration.ofMillis(acquireMs))) {
            return new LinkedList<>();
        }
        return doPoll();
    }

    /**
     * poll data
     *
     * @return
     */
    public abstract List<SourceRecord> doPoll();
}
