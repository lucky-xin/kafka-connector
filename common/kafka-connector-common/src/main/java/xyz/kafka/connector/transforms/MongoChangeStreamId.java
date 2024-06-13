package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;

import java.util.Map;

/**
 * MongoDB ChangeStream Key 转换
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class MongoChangeStreamId<R extends ConnectRecord<R>> implements Transformation<R> {


    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public R apply(R r) {
        Struct value = Requirements.requireStructOrNull(r.value(), r.topic());
        if (value == null) {
            return null;
        }
        String key = null;
        Object id = value.get("_id");
        if (id instanceof String k) {
            key = k;
        } else {
            key = id.toString();
        }
        return r.newRecord(r.topic(), r.kafkaPartition(), Schema.STRING_SCHEMA, key, r.valueSchema(),
                r.value(), r.timestamp(), r.headers());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }
}
