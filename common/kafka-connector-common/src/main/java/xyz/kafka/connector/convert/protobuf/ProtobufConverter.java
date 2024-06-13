package xyz.kafka.connector.convert.protobuf;

import cn.hutool.core.lang.Pair;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import xyz.kafka.serialization.protobuf.AbstractProtobufDeserializer;
import xyz.kafka.serialization.protobuf.AbstractProtobufSerializer;
import xyz.kafka.serialization.protobuf.ProtobufData;
import xyz.kafka.serialization.protobuf.ProtobufDataConfig;
import xyz.kafka.serialization.protobuf.ProtobufDeserializerConfig;
import xyz.kafka.serialization.protobuf.ProtobufSerializerConfig;

import java.io.IOException;
import java.util.Map;


/**
 * Implementation of Converter that uses Protobuf schemas and objects.
 * <p>
 * JsonSchema 数据转换器
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class ProtobufConverter implements Converter {

    private Serializer serializer;
    private Deserializer deserializer;

    private boolean isKey;
    private ProtobufData protobufData;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.serializer = new Serializer(configs);
        this.deserializer = new Deserializer(configs);
        this.protobufData = new ProtobufData(new ProtobufDataConfig(configs));
    }

    /**
     * 根据connector数据生成相应的字节数组。
     * 这是一个覆盖方法，它调用了另一个具有更多参数的同名方法。
     *
     * @param topic   连接的主题。
     * @param headers 连接的头部信息，包含一些元数据。
     * @param schema  连接数据的模式，用于定义数据的结构。
     * @param value   连接的实际数据值。
     * @return 返回一个字节数组，表示连接数据的序列化形式。
     */
    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        return fromConnectData(topic, headers, schema, value, isKey);
    }

    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value, boolean isKey) {
        try {
            String subjectName = serializer.getSubjectName(topic, isKey, null, null);
            SchemaMetadata meta = serializer.getLatestSchemaMetadata(subjectName);
            Pair<ProtobufSchema, Object> pair =
                    protobufData.fromConnectData(new ProtobufSchema(meta.getSchema()), schema, value);
            Object v = pair.getValue();
            if (v == null) {
                return new byte[0];
            } else if (v instanceof Message m) {
                return serializer.serialize(topic, isKey, headers, m, pair.getKey());
            } else {
                throw new DataException("Unsupported object of class " + v.getClass().getName());
            }
        } catch (SerializationException e) {
            throw new DataException(String.format(
                    "Failed to serialize Protobuf data from topic %s :",
                    topic
            ), e);
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
            );
        } catch (RestClientException | IOException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    /**
     * 根据提供的主题、模式和值，从connector中转换出字节数组。
     * 这是来自Connect数据的转换方法的重载版本，其中不包括连接配置的参数。
     *
     * @param topic  连接的主题。
     * @param schema 连接的数据模式。
     * @param value  连接的值，根据模式进行解析和转换。
     * @return 转换后的字节数组，代表了连接的数据。
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return fromConnectData(topic, null, schema, value);
    }

    /**
     * 将给定的主题和值转换为connector数据。
     * 该方法是toConnectData(String topic, String key, byte[] value)的重载版本，不考虑键值。
     *
     * @param topic 消息的主题。
     * @param value 消息的二进制值。
     * @return SchemaAndValue 对象，包含主题和转换后的值。
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return toConnectData(topic, null, value);
    }

    /**
     * 将给定的数据转换为connector数据格式。
     * 这是一个覆盖方法，它调用了另一个重载的toConnectData方法，提供了是否将数据作为键的标志。
     *
     * @param topic   消息的主题。
     * @param headers 消息的头部信息。
     * @param value   消息的正文数据。
     * @return SchemaAndValue 对象，包含了转换后的Schema和值数据。
     */
    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return toConnectData(topic, headers, value, isKey);
    }

    /**
     * 将消息转换为connector数据格式。
     *
     * @param topic   消息所属的主题。
     * @param headers 消息的头部信息。
     * @param value   消息的二进制值。
     * @param isKey   指示该消息值是否作为键。
     * @return SchemaAndValue 对象，包含消息的模式和值。如果无法解析，则返回NULL。
     * @throws DataException   当反序列化返回不支持的类型或发生异常时抛出。
     * @throws ConfigException 当配置无效，无法访问Protobuf数据时抛出。
     */
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value, boolean isKey) {
        try {
            Pair<ProtobufSchema, Object> pair = deserializer.deserialize(topic, isKey, headers, value);
            if (pair == null || pair.getValue() == null) {
                return SchemaAndValue.NULL;
            } else {
                Object object = pair.getValue();
                if (object instanceof Message message) {
                    return protobufData.toConnectData(pair.getKey(), message);
                }
                throw new DataException(String.format(
                        "Unsupported type %s returned during deserialization of topic %s ",
                        object.getClass().getName(),
                        topic
                ));
            }
        } catch (SerializationException e) {
            throw new DataException(String.format(
                    "Failed to deserialize data for topic %s to Protobuf: ",
                    topic
            ), e);
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
            );
        }
    }

    private static class Serializer extends AbstractProtobufSerializer<Message> {


        public Serializer(Map<String, ?> configs) {
            super.configure(new ProtobufSerializerConfig(configs));
        }

        public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this.schemaRegistry = client;
            super.configure(new ProtobufSerializerConfig(configs));
        }

        /**
         * 序列化消息的方法。
         *
         * @param topic   消息的主题。
         * @param isKey   指示消息是否作为键。
         * @param headers 消息的头部信息，包含额外的元数据。
         * @param value   消息的正文内容。
         * @param schema  消息的protobuf模式，用于指导序列化过程。
         * @return 返回序列化后的消息字节数组。
         */
        public byte[] serialize(String topic, boolean isKey, Headers headers, Message value, ProtobufSchema schema) {
            return serializeImpl(getSubjectName(topic, isKey, value, schema), topic, headers, schema, value);
        }

        @Override
        public String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
            return super.getSubjectName(topic, isKey, value, schema);
        }
    }

    private static class Deserializer extends AbstractProtobufDeserializer<Message> {

        public Deserializer(SchemaRegistryClient client) {
            schemaRegistry = client;
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            super.configure(new ProtobufDeserializerConfig(configs), null);
        }

        public Deserializer(Map<String, ?> configs) {
            super.configure(new ProtobufDeserializerConfig(configs), null);
        }

        /**
         * 反序列化给定的主题相关的数据。
         * 该方法将给定的负载（payload）和头信息（headers）反序列化为特定的Protobuf结构。
         *
         * @param topic   指定的主题名称，此主题关联的数据将被反序列化。
         * @param isKey   指示给定的负载是否为主题的关键数据。
         * @param headers 包含与给定负载相关联的头信息。
         * @param payload 需要被反序列化的数据。
         * @return 返回一个Pair对象，其中包含Protobuf模式和反序列化后的对象实例。
         */
        public Pair<ProtobufSchema, Object> deserialize(String topic, boolean isKey, Headers headers, byte[] payload) {
            return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
        }
    }
}
