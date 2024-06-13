package xyz.kafka.serialization.json;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Generic JSON deserializer.
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class JsonSchemaDeserializer<T> extends AbstractJsonSchemaDeserializer<T>
        implements Deserializer<T> {

    /**
     * Constructor used by Kafka consumer.
     */
    public JsonSchemaDeserializer() {
    }

    public JsonSchemaDeserializer(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    public JsonSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        this(client, props, null);
    }

    @VisibleForTesting
    public JsonSchemaDeserializer(
            SchemaRegistryClient client,
            Map<String, ?> props,
            Class<T> type
    ) {
        schemaRegistry = client;
        configure(deserializerConfig(props), type);
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        configure(new JsonSchemaDeserializerConfig(props), isKey);
    }

    @SuppressWarnings("unchecked")
    protected void configure(JsonSchemaDeserializerConfig config, boolean isKey) {
        this.isKey = isKey;
        if (isKey) {
            configure(
                    config,
                    (Class<T>) config.getClass(JsonSchemaDeserializerConfig.JSON_KEY_TYPE)
            );
        } else {
            configure(
                    config,
                    (Class<T>) config.getClass(JsonSchemaDeserializerConfig.JSON_VALUE_TYPE)
            );
        }
    }

    /**
     * 反序列化方法，将给定的主题和字节数据转换为指定类型的对象。
     * @param topic 主题名称，用于标识数据的来源或目的地。
     * @param bytes 表示要反序列化的数据的字节数组。
     * @return 反序列化后的对象，其类型取决于调用此方法的具体实现。
     */
    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] bytes) {
        // 将字节数据反序列化为对象，不考虑分区键，不传递上下文对象
        return (T) deserialize(false, topic, isKey, bytes, null);
    }

    /**
     * 反序列化方法，将给定的主题、头信息和字节数据转换为特定类型的对象。
     * @param topic 主题名称，标识数据的来源或目的地。
     * @param headers 消息头，包含与消息相关的信息，如键值对形式的元数据。
     * @param bytes 需要被反序列化的字节数据。
     * @return T 返回反序列化后的对象，其类型依赖于调用时的上下文。
     */
    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, Headers headers, byte[] bytes) {
        // 对象反序列化，将字节数据转换为特定类型
        return (T) deserialize(false, topic, isKey, bytes, headers);
    }
}
