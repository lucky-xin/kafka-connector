package xyz.kafka.connector.convert.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.saasquatch.jsonschemainferrer.FormatInferrer;
import com.saasquatch.jsonschemainferrer.FormatInferrers;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import xyz.kafka.schema.generator.JsonSchemaGenerator;
import xyz.kafka.serialization.AbstractKafkaSchemaSerDer;
import xyz.kafka.serialization.json.JsonData;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.serialization.json.JsonSchemaDeserializerConfig;
import xyz.kafka.serialization.json.JsonSchemaSerializerConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;

/**
 * json数据转换器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class JsonConverter implements Converter, AutoCloseable {

    private JsonSerializer serializer;
    private JsonDeserializer deserializer;
    private Cache<String, Schema> cache;
    private JsonData jsonData;
    private JsonSchemaGenerator jsonSchemaGenerator;
    private SubjectNameStrategy subjectNameStrategy;
    private int useSchemaId;
    private boolean isKey;
    private boolean autoRegisterSchemas;

    @Override
    public ConfigDef config() {
        return JsonConverterConfig.configDef();
    }

    public void configure(Map<String, ?> configs) {
        JsonConverterConfig config = new JsonConverterConfig(configs);
        if (config.cacheEnable()) {
            cache = Caffeine.newBuilder()
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .maximumSize(config.schemaCacheSize())
                    .softValues()
                    .build();
        }
        ObjectMapper objectMapper = new ObjectMapper();
        if (config.useBigDecimalForFloats()) {
            objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        }
        if (config.writeBigDecimalAsPlain()) {
            objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        }
        isKey = config.type() == ConverterType.KEY;
        serializer = new JsonSerializer(objectMapper);
        serializer.configure(configs, isKey);
        deserializer = new JsonDeserializer(objectMapper);
        deserializer.configure(configs, isKey);
        jsonData = new JsonData(new JsonDataConfig(configs));
        List<FormatInferrer> formatInferrers = new ArrayList<>();
        if (config.schemaGenDateTimeInferEnable()) {
            formatInferrers.add(FormatInferrers.dateTime());
        }
        if (config.schemaGenEmailInferEnable()) {
            formatInferrers.add(FormatInferrers.email());
        }
        if (config.schemaGenIpInferEnable()) {
            formatInferrers.add(FormatInferrers.ip());
        }
        this.useSchemaId = config.useSchemaId();
        this.autoRegisterSchemas = config.autoRegisterSchemas();
        this.jsonSchemaGenerator = new JsonSchemaGenerator(false, false, false, formatInferrers);
        this.subjectNameStrategy = config.subjectNameStrategy();
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(ConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    @Override
    public void close() {
        Utils.closeQuietly(this.serializer, "JSON converter serializer");
        Utils.closeQuietly(this.deserializer, "JSON converter deserializer");
    }

    /**
     * 根据给定的主题、模式和值，从连接数据中转换成字节数组。
     *
     * @param topic  主题，表示数据的主题标识。
     * @param schema 模式，描述数据的结构。
     * @param value  值，需要转换的数据。
     * @return 返回转换后的字节数组。如果模式和值都为null，则返回空字节数组。
     * @throws DataException 如果序列化过程中发生错误，则抛出数据异常。
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        // 如果模式和值都为null，直接返回空字节数组
        if (schema == null && value == null) {
            return new byte[0];
        }
        try {
            // 使用jsonData从连接数据中转换成JsonNode
            JsonNode jsonNode = jsonData.fromConnectData(schema, value, true);
            // 使用序列化器将JsonNode序列化成字节数组
            return serializer.serialize(topic, jsonNode);
        } catch (SerializationException e) {
            // 如果序列化过程中发生错误，抛出数据异常
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * 将给定的主题和字节值转换为Kafka Connect的数据格式。
     *
     * @param topic Kafka主题名称，用于标识数据的来源。
     * @param value 主题相关的字节数据。
     * @return SchemaAndValue对象，包含转换后的数据的模式和值。
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        // 如果值为空或长度为0，则返回NULL
        if (value == null || value.length == 0) {
            return SchemaAndValue.NULL;
        }
        JsonNode jsonValue;
        try {
            Schema schema = null;
            // 反序列化字节数据为JsonNode
            jsonValue = deserializer.deserialize(topic, value);
            // 如果设置了主题名称策略，则使用该策略获取主题名称
            if (subjectNameStrategy != null) {
                String subjectName = subjectNameStrategy.subjectName(topic, isKey, null);
                JsonSchema js = null;
                // 如果指定了使用schema ID，则通过ID获取schema；否则，获取最新的schema元数据
                if (useSchemaId != -1) {
                    js = ((JsonSchema) deserializer.schemaRegistry().getSchemaBySubjectAndId(subjectName, useSchemaId));
                } else {
                    SchemaMetadata meta = deserializer.schemaRegistry().getLatestSchemaMetadata(subjectName);
                    js = new JsonSchema(meta.getSchema());
                }
                // 根据获取的JsonSchema转换为Kafka Connect的模式
                schema = jsonData.toConnectSchema(js, Map.of());
            } else {
                // 如果没有设置主题名称策略，则直接获取模式
                schema = getSchema(topic, jsonValue);
            }
            // 使用获取的模式和反序列化的JsonNode数据，转换为Kafka Connect的数据格式
            return new SchemaAndValue(schema, JsonData.toConnectData(schema, jsonValue));

        } catch (Exception e) {
            // 如果转换过程中发生异常，则抛出数据异常
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }
    }

    private Schema getSchema(String topic, JsonNode jsonValue) {
        if (cache != null) {
            return cache.get(topic, k -> {
                ObjectNode on = this.jsonSchemaGenerator.toSchema(jsonValue);
                JsonSchema js = new JsonSchema(on);
                if (autoRegisterSchemas) {
                    register(topic, js);
                }
                return jsonData.toConnectSchema(new JsonSchema(on), Map.of());
            });
        }
        ObjectNode on = this.jsonSchemaGenerator.toSchema(jsonValue);
        JsonSchema js = new JsonSchema(on);
        if (autoRegisterSchemas) {
            register(topic, js);
        }
        return jsonData.toConnectSchema(js, Map.of());
    }

    private void register(String topic, JsonSchema js) {
        try {
            deserializer.schemaRegistry().register(topic, js, true);
        } catch (Exception e) {
            throw new DataException("Failed to register schema");
        }
    }

    public static class JsonSerializer extends AbstractKafkaSchemaSerDer implements Serializer<JsonNode> {
        public static final String JSON_OBJECT_MAPPER_PREFIX_CONFIG = "json.object.mapper.ser";

        private final ObjectMapper objectMapper;

        /**
         * Default constructor needed by Kafka
         */
        public JsonSerializer() {
            this(new ObjectMapper());
        }


        JsonSerializer(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            JsonSchemaDeserializerConfig jsonSchemaConfig = new JsonSchemaDeserializerConfig(configs);
            super.configureClientProperties(jsonSchemaConfig, new JsonSchemaProvider());
            Serializer.super.configure(configs, isKey);
            configs.entrySet()
                    .stream()
                    .filter(e -> e.getKey().startsWith(JSON_OBJECT_MAPPER_PREFIX_CONFIG))
                    .forEach(e -> {
                        String name = e.getKey().substring(JSON_OBJECT_MAPPER_PREFIX_CONFIG.length());
                        Stream.of(SerializationFeature.values())
                                .filter(sf -> sf.name().equalsIgnoreCase(name))
                                .findFirst()
                                .ifPresent(sf -> objectMapper.configure(sf, Boolean.parseBoolean(e.getValue().toString())));
                    });
        }

        /**
         * 将给定的 JSON 数据节点序列化为字节数组。
         *
         * @param topic 相关的主题，此参数在序列化过程中可能不直接使用，但有助于提供上下文信息。
         * @param data  要序列化的 JSON 数据节点。如果数据为 null，则返回一个空的字节数组。
         * @return 序列化后的字节数组。如果输入数据为 null，则返回长度为 0 的字节数组。
         * @throws SerializationException 如果在序列化过程中发生错误，将抛出此异常。
         */
        @Override
        public byte[] serialize(String topic, JsonNode data) {
            // 如果数据节点为空，则直接返回空字节数组
            if (data == null) {
                return new byte[0];
            }

            try {
                // 使用 ObjectMapper 将 JSON 数据节点序列化为字节数组
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }
    }

    public static class JsonDeserializer extends AbstractKafkaSchemaSerDer implements Deserializer<JsonNode> {
        public static final String JSON_OBJECT_MAPPER_PREFIX_CONFIG = "json.object.mapper.der";
        private final ObjectMapper objectMapper;

        /**
         * Default constructor needed by Kafka
         */
        public JsonDeserializer() {
            this(new ObjectMapper());
        }

        /**
         * A constructor that additionally specifies some {@link DeserializationFeature}s
         * for the deserializer
         *
         * @param objectMapper ObjectMapper
         */
        JsonDeserializer(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            Deserializer.super.configure(configs, isKey);
            JsonSchemaSerializerConfig jsonSchemaConfig = new JsonSchemaSerializerConfig(configs);
            super.configureClientProperties(jsonSchemaConfig, new JsonSchemaProvider());
            configs.entrySet()
                    .stream()
                    .filter(e -> e.getKey().startsWith(JSON_OBJECT_MAPPER_PREFIX_CONFIG))
                    .forEach(e -> {
                        String name = e.getKey().substring(JSON_OBJECT_MAPPER_PREFIX_CONFIG.length());
                        Stream.of(DeserializationFeature.values())
                                .filter(sf -> sf.name().equalsIgnoreCase(name))
                                .findFirst()
                                .ifPresent(sf -> objectMapper.configure(sf, Boolean.parseBoolean(e.getValue().toString())));
                    });
        }

        /**
         * 反序列化函数：将给定的主题和字节数组转换为JsonNode。
         *
         * @param topic 主题字符串，表示待反序列化的数据的主题。
         * @param bytes 表示Json数据的字节数组。如果数据为空或数组长度为0，则返回null。
         * @return JsonNode 如果反序列化成功，则返回相应的JsonNode对象；如果输入字节数组为空，则返回null。
         * @throws SerializationException 如果在反序列化过程中发生任何异常，则抛出此异常。
         */
        @Override
        public JsonNode deserialize(String topic, byte[] bytes) {
            // 检查输入字节数组是否为空
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try {
                // 尝试将字节数组反序列化为JsonNode
                return objectMapper.readTree(bytes);
            } catch (Exception e) {
                // 如果反序列化过程中发生异常，抛出序列化异常
                throw new SerializationException(e);
            }
        }
    }
}
