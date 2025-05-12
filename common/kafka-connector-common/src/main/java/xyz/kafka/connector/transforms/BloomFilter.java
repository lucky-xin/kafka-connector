package xyz.kafka.connector.transforms;

import com.alibaba.fastjson2.JSON;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.config.RedissonConfig;
import xyz.kafka.utils.StringUtil;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 基于Redis布隆过滤器做消息去重
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-07
 */
public class BloomFilter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final Logger log = LoggerFactory.getLogger(BloomFilter.class);
    public static final String BLOOM_FILTER_KEY = "bloom.filter.key";
    public static final String BLOOM_FILTER_CAPACITY = "bloom.filter.capacity";
    public static final String BLOOM_FILTER_ERROR_RATE = "bloom.filter.error.rate";
    public static final String BLOOM_FILTER_EXPIRE_SECONDS = "bloom.filter.expire.seconds";

    private long capacity;
    private long expire;
    private double errorRate;
    private RedissonClient redisson;

    private String filterKey;
    private String lockKey;

    private RBloomFilter<String> filter;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    BLOOM_FILTER_KEY,
                    ConfigDef.Type.STRING,
                    "bloom:filter",
                    ConfigDef.Importance.MEDIUM,
                    "Bloom filter."
            ).define(
                    BLOOM_FILTER_CAPACITY,
                    ConfigDef.Type.LONG,
                    StringUtil.toLong(System.getenv("BLOOM_FILTER_CAPACITY"), 1000000L),
                    ConfigDef.Importance.MEDIUM,
                    "Bloom filter capacity."
            ).define(
                    BLOOM_FILTER_ERROR_RATE,
                    ConfigDef.Type.DOUBLE,
                    StringUtil.toDouble(System.getenv("BLOOM_FILTER_ERROR_RATE"), 0.002),
                    ConfigDef.Importance.MEDIUM,
                    "Bloom filter error rate."
            ).define(
                    BLOOM_FILTER_EXPIRE_SECONDS,
                    ConfigDef.Type.LONG,
                    StringUtil.toLong(System.getenv("BLOOM_FILTER_EXPIRE_SECONDS"), 60 * 60 * 24L),
                    ConfigDef.Importance.MEDIUM,
                    "Bloom filter expire seconds."

            );

    @Override
    public R apply(R r) {
        init();
        String key;
        if (r.key() instanceof String s) {
            key = s;
        } else if (r.key() instanceof Struct struct) {
            key = struct.toString();
        } else {
            key = JSON.toJSONString(r.key());
        }
        key = StringUtil.md5(key);
        try {
            if (!this.filter.add(key)) {
                return null;
            }
        } catch (Exception e) {
            throw new RetriableException(e);
        }
        return r;
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        filter.clearExpire();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.capacity = config.getLong(BLOOM_FILTER_CAPACITY);
        this.errorRate = config.getDouble(BLOOM_FILTER_ERROR_RATE);
        this.expire = config.getLong(BLOOM_FILTER_EXPIRE_SECONDS);
        this.redisson = new RedissonConfig(configs).getRedisson();
        this.filterKey = config.getString(BLOOM_FILTER_KEY);
        this.lockKey = "distributed_lock:" + filterKey;
    }

    private void init() {
        this.filter = redisson.getBloomFilter(this.filterKey);
        if (this.filter.isExists()) {
            return;
        }

        RLock lock = this.redisson.getLock(this.lockKey);
        long waiteTime = 10000;
        try {
            lock.lock(waiteTime, TimeUnit.MILLISECONDS);
            if (this.filter.isExists()) {
                return;
            }
            if (this.filter.tryInit(this.capacity, this.errorRate)) {
                this.redisson.getKeys().expireAsync(this.filterKey, this.expire, TimeUnit.SECONDS);
                log.info("inited bloom filter:{}", this.filterKey);
            }
        } finally {
            lock.unlock();
        }
    }
}
