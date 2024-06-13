package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

/**
 * id 字符串转换成结构体
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class StringIdToStruct<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String TARGET_NAME = "target.name";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TARGET_NAME, ConfigDef.Type.STRING, "id", ConfigDef.Importance.MEDIUM,
                    "Target id name");
    private String targetName;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.targetName = config.getString(TARGET_NAME);

    }

    @Override
    public R apply(R r) {
        if (r.key() instanceof String s) {
            Schema schema = SchemaBuilder.struct()
                    .field(targetName, Schema.STRING_SCHEMA)
                    .build();
            Struct struct = new Struct(schema)
                    .put(targetName, s);
            return r.newRecord(
                    r.topic(), r.kafkaPartition(), schema, struct, r.valueSchema(),
                    r.value(), r.timestamp(), r.headers());
        }
        return r;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // empty
    }
}
