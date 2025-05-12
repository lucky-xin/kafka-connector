package xyz.kafka.connector.converter.json;

import cn.hutool.core.lang.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import xyz.kafka.serialization.json.AbstractJsonSchemaDeserializer;
import xyz.kafka.serialization.json.AbstractJsonSchemaSerializer;
import xyz.kafka.serialization.json.JsonData;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.serialization.json.JsonSchemaDeserializerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * JsonSchemaConverter 数据转换器
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class JsonSchemaConverter implements Converter, Closeable {

    private Serializer serializer;
    private Deserializer deserializer;

    private boolean isKey;

    private JsonData jsonData;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.jsonData = new JsonData(new JsonDataConfig(configs));
        this.serializer = new Serializer(configs);
        this.deserializer = new Deserializer(configs);
    }

    /**
     * 将来自connector的数据转换为字节数组。
     * 这个方法是 {@link Converter#fromConnectData(String, Schema, Object)} 的重载版本，不指定 group ID。
     *
     * @param topic  与数据关联的主题。
     * @param schema 数据值的模式。
     * @param value  需要转换的数据值。
     * @return 转换后的字节数组。
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return fromConnectData(topic, null, schema, value);
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

    /**
     * 根据提供的connector数据生成字节数组。
     * 该方法主要用于将Kafka Connect的数据转换为字节数组形式，以便于在Kafka中进行存储或传输。
     *
     * @param topic   Kafka主题，即数据将要被发送到或已经发送自的主题。
     * @param headers 消息头，包含与消息相关联的元数据。
     * @param schema  数据的模式或结构，用于验证和解析数据。
     * @param value   需要被转换为字节数组的实际数据。
     * @param isKey   指示提供的数据是否为消息键。消息键用于标识消息的唯一性。
     * @return 返回一个字节数组，表示转换后的数据。如果schema和value都为null，则返回空字节数组。
     * @throws DataException         如果数据转换过程中发生序列化错误，则抛出此异常。
     * @throws ConfigException       如果在访问JSON Schema数据时配置不正确，则抛出此异常。
     * @throws IllegalStateException 如果转换过程中发生非预期的异常，则抛出此异常。
     */
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value, boolean isKey) {
        // 如果schema和value都为null，直接返回空字节数组
        if (schema == null && value == null) {
            return new byte[0];
        }
        try {
            // 获取序列化主题名称
            String subjectName = serializer.getSubjectName(topic, isKey, null, null);
            // 将数据根据schema转换为JsonNode
            JsonNode jsonValue = jsonData.fromConnectData(schema, value, true);
            // 序列化数据为字节数组
            return serializer.serialize(subjectName, topic, headers, jsonValue);
        } catch (SerializationException e) {
            // 处理序列化异常
            throw new DataException(String.format("Converting Kafka Connect data to byte[] failed due to "
                    + "serialization error of topic %s: ", topic), e);
        } catch (InvalidConfigurationException e) {
            // 处理配置异常
            throw new ConfigException(
                    String.format("Failed to access JSON Schema data from topic %s : %s", topic, e.getMessage()));
        } catch (Exception e) {
            // 处理其他异常
            throw new IllegalStateException(e);
        }
    }

    /**
     * 将给定的主题和值转换为连接数据。
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
     * 将数据转换为connector数据格式。
     * 这个方法是toConnectData(String topic, byte[] payload)的扩展，增加了headers参数。
     * 主要用于将接收到的消息转换为Kafka Connect格式，以便进一步处理。
     *
     * @param topic   与数据相关联的主题。
     * @param headers 消息头，包含额外的消息元数据。
     * @param payload 要转换的数据体。
     * @return 返回转换后的SchemaAndValue对象，包含数据的模式和值。
     */
    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] payload) {
        return toConnectData(topic, headers, payload, isKey);
    }

    /**
     * 将给定的主题、头信息、有效载荷和键标志转换为Kafka Connect数据格式。
     *
     * @param topic 消息的主题。
     * @param headers 消息的头信息。
     * @param payload 消息的有效载荷，字节数组形式。
     * @param isKey 指示有效载荷是否为键的有效载荷。
     * @return SchemaAndValue 对象，包含转换后的数据的模式和值。
     * @throws DataException 当转换字节数组到Kafka Connect数据时发生序列化错误。
     * @throws ConfigException 当访问JSON模式数据失败时抛出。
     */
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] payload, boolean isKey) {
        // 如果有效载荷为null或空，则返回NULL SchemaAndValue对象
        if ((payload == null || payload.length == 0)) {
            return SchemaAndValue.NULL;
        }
        try {
            // 使用反序列化器将有效载荷转换为JsonSchema和对应的对象
            Pair<JsonSchema, Object> pair = deserializer.deserialize(topic, isKey, payload, headers);
            JsonSchema jsonSchema = pair.getKey();
            // 将JsonSchema转换为Kafka Connect兼容的模式
            Schema schema = jsonData.toConnectSchema(jsonSchema, Collections.emptyMap());
            JsonNode rawVal = (JsonNode) pair.getValue();
            // 将根据模式转换后的JsonNode对象转换为Kafka Connect数据格式
            Object connectData = JsonData.toConnectData(schema, rawVal);

            // 如果转换后的数据是Struct类型，则以Struct的形式返回
            if (connectData instanceof Struct struct) {
                return new SchemaAndValue(struct.schema(), struct);
            }
            // 否则以普通的Schema和值形式返回
            return new SchemaAndValue(schema, connectData);
        } catch (SerializationException e) {
            throw new DataException(String.format("Converting byte[] to Kafka Connect data failed due to "
                    + "serialization error of topic %s: ", topic), e);
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(String.format("Failed to access JSON Schema data from "
                    + "topic %s : %s", topic, e.getMessage())
            );
        }
    }


    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(deserializer);
        IOUtils.closeQuietly(serializer);
    }

    private static class Serializer extends AbstractJsonSchemaSerializer<Object> {

        public Serializer(Map<String, ?> configs) {
            super.configure(configs);
        }

        public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
            schemaRegistry = client;
            super.configure(configs);
        }

        /**
         * 序列化给定的主题数据。
         *
         * @param subjectName 主题名称，指定数据的主题。
         * @param topic 数据所属的具体话题。
         * @param headers 包含与数据一同发送的额外信息的头部集合。
         * @param value 需要被序列化的实际数据对象。
         * @return 返回序列化后的数据字节数组。
         */
        public byte[] serialize(String subjectName, String topic, Headers headers, Object value) {
            // 调用序列化实现方法进行实际的序列化操作
            return serializeImpl(subjectName, topic, headers, value);
        }

        @Override
        public String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
            return super.getSubjectName(topic, isKey, value, schema);
        }
    }

    private static class Deserializer extends AbstractJsonSchemaDeserializer<Pair<JsonSchema, Object>> {

        public Deserializer(SchemaRegistryClient client) {
            schemaRegistry = client;
        }

        public Deserializer(Map<String, ?> configs) {
            super.configure(new JsonSchemaDeserializerConfig(configs), null);
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            super.configure(new JsonSchemaDeserializerConfig(configs), null);
        }

        /**
         * 反序列化给定的主题数据。
         * 该方法将给定的主题、键标志、有效载荷和头信息反序列化为 JsonSchema 和关联的对象实例。
         *
         * @param topic 主题名称，标识数据的来源或归属。
         * @param isKey 指示当前数据是否为键值。
         * @param payload 用于反序列化的二进制数据。
         * @param headers 包含与数据一起发送的额外元数据的头信息。
         * @return Pair<JsonSchema, Object> 包含反序列化后的 JsonSchema 和对象实例的键值对。
         */
        public Pair<JsonSchema, Object> deserialize(String topic, boolean isKey, byte[] payload, Headers headers) {
            return deserializeWithSchemaAndVersion(topic, isKey, payload, headers);
        }
    }

}
