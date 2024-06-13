package xyz.kafka.serialization.protobuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * ProtobufDeserializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class ProtobufDeserializer<T extends Message>
        extends AbstractProtobufDeserializer<T> implements Deserializer<T> {

    /**
     * Constructor used by Kafka consumer.
     */
    public ProtobufDeserializer() {

    }

    public ProtobufDeserializer(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    public ProtobufDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        this(client, props, null);
    }

    @VisibleForTesting
    public ProtobufDeserializer(SchemaRegistryClient client,
                                Map<String, ?> props,
                                Class<T> type) {
        schemaRegistry = client;
        configure(deserializerConfig(props), type);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(new ProtobufDeserializerConfig(configs), isKey);
    }

    @SuppressWarnings("unchecked")
    protected void configure(ProtobufDeserializerConfig config, boolean isKey) {
        this.isKey = isKey;
        if (isKey) {
            configure(
                    config,
                    (Class<T>) config.getClass(ProtobufDeserializerConfig.SPECIFIC_PROTOBUF_KEY_TYPE)
            );
        } else {
            configure(
                    config,
                    (Class<T>) config.getClass(ProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE)
            );
        }
    }

    /**
     * 反序列化方法，用于将给定的主题、头信息和字节数据反序列化成特定的对象。
     *
     * @param topic 主题名称，标识数据的来源或目的地。
     * @param headers 消息头，包含额外的非数据信息。
     * @param bytes 需要被反序列化的字节数据。
     * @return 反序列化后的对象，其类型取决于泛型T的实参。
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public T deserialize(String topic, Headers headers, byte[] bytes) {
        // 调用另一个重载的deserialize方法进行实际的反序列化操作
        return (T) deserialize(true, topic, isKey, headers, bytes);
    }

    /**
     * 反序列化方法，将给定的主题和字节数据转换为泛型类型T的对象。
     * @param topic 主题名称，标识了需要反序列化的数据的来源。
     * @param bytes 表示待反序列化数据的字节数组。
     * @return 泛型类型T的对象，该对象是根据给定的字节数组反序列化得到的。
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public T deserialize(String topic, byte[] bytes) {
        // 调用另一个重载的deserialize方法进行实际的反序列化操作
        return (T) deserialize(true, topic, isKey, null, bytes);
    }

    @Override
    public void close() {

    }
}
