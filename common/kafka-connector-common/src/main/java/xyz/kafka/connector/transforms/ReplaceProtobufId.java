package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 * 替换id
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class ReplaceProtobufId<R extends ConnectRecord<R>> implements Transformation<R> {

    protected static final byte MAGIC_BYTE = 0x0;

    private static final String NEW_ID = "new.id";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    NEW_ID,
                    ConfigDef.Type.INT,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    "new id");
    private Integer newId;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.newId = config.getInt(NEW_ID);
    }

    @Override
    public R apply(R r) {
        if (r.value() instanceof byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            if (buffer.get() == MAGIC_BYTE) {
                ByteBuffer bb = ByteBuffer.allocate(bytes.length);
                bb.put(MAGIC_BYTE);
                bb.putInt(newId);
                bb.put(Arrays.copyOfRange(bytes, 5, bytes.length));
                return r.newRecord(
                        r.topic(), r.kafkaPartition(), r.keySchema(), r.key(),
                        r.valueSchema(), bb.array(), r.timestamp(), r.headers());
            }

        }
        return r;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }
}
