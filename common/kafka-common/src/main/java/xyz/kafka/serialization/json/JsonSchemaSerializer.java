package xyz.kafka.serialization.json;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * JsonSchemaSerializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class JsonSchemaSerializer<T> extends AbstractJsonSchemaSerializer<T> implements Serializer<T> {

    /**
     * Constructor used by Kafka producer.
     */
    public JsonSchemaSerializer() {

    }


    public JsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        this(client, props, DEFAULT_CACHE_CAPACITY);

    }

    public JsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props, int cacheCapacity) {
        schemaRegistry = client;
        configure(props);
    }

    @Override
    public void configure(Map<String, ?> orig, boolean isKey) {
        this.isKey = isKey;
        configure(orig);
    }

    /**
     * 对给定的主题、头部和数据进行序列化。
     *
     * @param topic 主题名称，用于标识数据的类型或目的。
     * @param headers 消息头，包含额外的元数据。
     * @param data 需要被序列化的数据对象。如果数据为null，则返回空字节数组。
     * @return 序列化后的数据字节数组。如果输入数据为null，则返回长度为0的字节数组。
     * @throws IllegalStateException 如果序列化过程中发生异常。
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (data == null) {
            // 如果数据对象为null，直接返回空字节数组
            return new byte[0];
        }
        try {
            // 获取序列化主题名称
            String subjectName = getSubjectName(topic, isKey, null, null);
            // 提取数据对象的实际值
            Object value = JsonSchemaUtils.getValue(data);
            // 执行实际的序列化操作
            return serializeImpl(subjectName, topic, headers, (T) value);
        } catch (Exception e) {
            // 任何序列化过程中的异常都将被转换为IllegalStateException抛出
            throw new IllegalStateException(e);
        }
    }

    /**
     * 序列化给定的主题和对象。
     *
     * @param topic 主题名称，表示数据的分类或标识。
     * @param t 需要被序列化的对象。
     * @return 序列化后的数据，以字节数组的形式返回。
     * @param <T> 序列化对象的类型。
     */
    @Override
    public byte[] serialize(String topic, T t) {
        // 调用另一个重载的serialize方法，进行序列化处理
        return serialize(topic, null, t);
    }
}
