package xyz.kafka.connector.transforms;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Common functionality used by all Transformation implementations.
 *
 * @author luchaoxin
 */
abstract class AbstractTransformation<T extends ConnectRecord<T>> implements KeyOrValueTransformation<T> {

    private final ConfigDef configDef;

    protected boolean isSchemaCache;
    protected Cache<String, Schema> schemaCache;

    public static final String CACHE_SCHEMA = "cache.target.schema";

    protected AbstractTransformation(ConfigDef configDef) {
        this.configDef = configDef.define(
                CACHE_SCHEMA,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                "cache target schema or not"
        );
    }

    protected abstract void configure(Map<String, ?> configs, AbstractConfig config);

    /**
     * Each transformation needs to parse the config using SimpleConfig.
     * <p>
     * It is done here to reduce duplicate code.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new SimpleConfig(config(), configs);
        this.configure(configs, config);
        this.isSchemaCache = Boolean.TRUE.equals(config.getBoolean(CACHE_SCHEMA));
        if (this.isSchemaCache) {
            schemaCache = Caffeine.newBuilder()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .maximumSize(1000)
                    .softValues()
                    .build();
        }
    }

    @Override
    public ConfigDef config() {
        return configDef;
    }

    /**
     * Nothing to close by default. Override if needed.
     */
    @Override
    public void close() {
    }
}
