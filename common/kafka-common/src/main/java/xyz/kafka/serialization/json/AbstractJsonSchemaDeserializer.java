package xyz.kafka.serialization.json;

import cn.hutool.core.lang.Pair;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import xyz.kafka.serialization.AbstractKafkaSchemaSerDer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


/**
 * AbstractJsonSchemaDeserializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public abstract class AbstractJsonSchemaDeserializer<T> extends AbstractKafkaSchemaSerDer {
    protected ObjectMapper objectMapper = Jackson.newObjectMapper();

    protected Class<T> type;
    protected String typeProperty;
    protected boolean validate;

    /**
     * Sets properties for this deserializer without overriding the schema registry client itself.
     * Useful for testing, where a mock client is injected.
     */
    protected void configure(JsonSchemaDeserializerConfig config, Class<T> type) {
        configureClientProperties(config, new JsonSchemaProvider());
        this.type = type;
        boolean failUnknownProperties = config.getBoolean(JsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failUnknownProperties);
        this.objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false);
        this.validate = config.getBoolean(JsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA);
        this.typeProperty = config.getString(JsonSchemaDeserializerConfig.TYPE_PROPERTY);
    }

    protected JsonSchemaDeserializerConfig deserializerConfig(Map<String, ?> props) {
        try {
            return new JsonSchemaDeserializerConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
    }

    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    /**
     * Deserializes the payload without including schema information for primitive types, maps, and
     * arrays. Just the resulting deserialized object is returned.
     *
     * <p>This behavior is the norm for Decoders/Deserializers.
     *
     * @param payload serialized data
     * @return the deserialized object
     */
    @SuppressWarnings({"unchecked"})
    protected T deserialize(byte[] payload)
            throws SerializationException, InvalidConfigurationException {
        return (T) deserialize(false, null, isKey, payload, null);
    }

    // The Object return type is a bit messy, but this is the simplest way to have
    // flexible decoding and not duplicate deserialization code multiple times for different variants.

    private String getTypeName(Schema schema, JsonNode jsonNode) {
        if (schema instanceof CombinedSchema c) {
            for (Schema subschema : c.getSubschemas()) {
                boolean valid = false;
                try {
                    subschema.validate(jsonNode);
                    valid = true;
                } catch (Exception e) {
                    // noop
                }
                if (valid) {
                    return getTypeName(subschema, jsonNode);
                }
            }
        } else if (schema instanceof ReferenceSchema r) {
            return getTypeName(r.getReferredSchema(), jsonNode);
        }
        return (String) schema.getUnprocessedProperties().get(typeProperty);
    }

    private Object deriveType(
            ByteBuffer buffer, int length, int start, String typeName
    ) throws IOException {
        try {
            Class<?> cls = Class.forName(typeName);
            return objectMapper.readValue(buffer.array(), start, length, cls);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + typeName + " could not be found.");
        }
    }

    private Object deriveType(JsonNode jsonNode, String typeName) {
        try {
            Class<?> cls = Class.forName(typeName);
            return objectMapper.convertValue(jsonNode, cls);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + typeName + " could not be found.");
        }
    }

    /**
     * 获取指定主题和ID的schema的版本号。
     *
     * @param topic   消息主题。
     * @param isKey   指示消息值是否为键。
     * @param id      schema的ID。
     * @param subject schema的主题名称。如果使用了废弃的主题命名策略，会通过getSubjectName重新计算。
     * @param schema  JsonSchema对象，提供给获取主题schema时使用，如果为null，则从schemaRegistry中获取。
     * @param value   消息的值，用于计算主题名称（如果需要）。
     * @return schema的版本号。
     * @throws SerializationException 如果获取schema或版本号过程中发生错误。
     */
    private Integer schemaVersion(
            String topic, boolean isKey, int id, String subject, JsonSchema schema, Object value
    ) throws SerializationException {
        int version;
        // 检查是否使用了废弃的主题命名策略，如果是，则重新计算主题名称
        if (isDeprecatedSubjectNameStrategy(isKey)) {
            subject = getSubjectName(topic, isKey, value, schema);
        }
        try {
            // 通过主题和ID从schemaRegistry获取schema，并获取该schema的版本号
            JsonSchema subjectSchema = (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
            version = schemaRegistry.getVersion(subject, subjectSchema);
        } catch (Exception e) {
            throw new SerializationException("get jsonschema error", e);
        }
        return version;
    }

    /**
     * 根据给定的主题、是否为主键、以及来自注册表的JSON模式，生成主题名称。
     * 如果当前使用的是弃用的主题名称策略，则返回null；否则，根据提供的参数生成并返回主题名称。
     *
     * @param topic              主题名称，是生成主题名称的基础。
     * @param isKey              表示该主题是否为主键主题。
     * @param schemaFromRegistry 来自注册表的JSON模式，用于可能的主题名称生成过程。
     * @return 如果当前使用的是弃用的主题名称策略，则返回null；否则返回生成的主题名称。
     */
    private String subjectName(String topic, boolean isKey, JsonSchema schemaFromRegistry) {
        // 判断是否使用弃用的主题名称策略，如果是则返回null，否则调用getSubjectName方法生成主题名称
        return isDeprecatedSubjectNameStrategy(isKey)
                ? null
                : getSubjectName(topic, isKey, null, schemaFromRegistry);
    }


    /**
     * 根据给定的参数获取用于反序列化的JSON模式。
     *
     * @param id                 schema的ID，用于从schema注册表中获取schema。
     * @param schemaFromRegistry 从注册表中获取的schema，如果主题不废弃，则可能使用此schema。
     * @param subject            主题名称，用于从schema注册表中获取schema。
     * @param isKey              指示主题是否作为键的布尔值。
     * @return 返回一个JsonSchema对象，用于反序列化。
     * @throws IOException         当与schema注册表的通信出现问题时抛出。
     * @throws RestClientException 当schema注册表服务端返回错误时抛出。
     */
    private JsonSchema schemaForDeserialize(
            int id, JsonSchema schemaFromRegistry, String subject, boolean isKey
    ) throws IOException, RestClientException {
        // 判断是否使用废弃的主题名称策略，如果是，则返回schemaFromRegistry的副本，否则通过主题和ID从注册表获取schema
        return isDeprecatedSubjectNameStrategy(isKey)
                ? JsonSchemaUtils.copyOf(schemaFromRegistry)
                : (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
    }


    /**
     * 使用指定的schema和版本号反序列化数据。
     * 该方法会根据提供的topic、是否为key、数据负载和头部信息来反序列化数据，并返回一个包含JsonSchema和反序列化后对象的Pair。
     *
     * @param topic   消息的主题，用于确定反序列化时使用的schema。
     * @param isKey   表示提供的payload是否为键。
     * @param payload 要反序列化的数据负载。
     * @param headers 包含额外的元数据的头部信息，可能用于反序列化过程中。
     * @return 一个Pair对象，包含反序列化所用的JsonSchema和反序列化后的对象。
     * @throws SerializationException 如果反序列化过程中发生错误。
     */
    @SuppressWarnings({"unchecked"})
    public Pair<JsonSchema, Object> deserializeWithSchemaAndVersion(
            String topic, boolean isKey, byte[] payload, Headers headers) throws SerializationException {
        // 直接调用deserialize方法进行反序列化，并将结果转换为Pair<JsonSchema, Object>类型
        return (Pair<JsonSchema, Object>) deserialize(true, topic, isKey, payload, headers);
    }

    /**
     * 反序列化函数，用于将给定的字节数据转换为对象。此函数能够处理 JSON 数据，并依据提供的主题和元数据从schema注册表中获取相应的schema来验证和解析数据。
     *
     * @param schemaAndVersion 是否要求返回schema和版本信息。如果为true，则函数返回一个包含schema和反序列化对象的Pair；否则，只返回反序列化对象。
     * @param topic            Kafka消息的主题。
     * @param isKey            指示当前处理的是消息的关键字段还是值。
     * @param payload          要反序列化的字节数据。
     * @param headers          消息的头部信息，可能包含额外的元数据用于反序列化过程。
     * @return 如果schemaAndVersion为true，则返回一个Pair，包含schema和反序列化的对象；否则，只返回反序列化的对象。
     * @throws SerializationException        如果反序列化过程中发生错误，比如数据不符合schema。
     * @throws InvalidConfigurationException 如果未配置必要的schema注册表客户端。
     */
    protected Object deserialize(boolean schemaAndVersion, String topic, boolean isKey, byte[] payload, Headers headers)
            throws SerializationException, InvalidConfigurationException {
        // 检查payload是否为空，为空则直接返回null
        if (payload == null || payload.length == 0) {
            return null;
        }
        int id = -1;
        try {
            // 从payload中解析出schema ID
            ByteBuffer buffer = getByteBuffer(payload);
            id = buffer.getInt();
            String subject = strategyUsesSchema(isKey)
                    ? getContextName(topic) : subjectName(topic, isKey, null);

            // 尝试根据不同的配置获取reader schema
            JsonSchema readerSchema = getReadSchema(subject);
            // 解析JSON节点
            int start = buffer.position() + buffer.arrayOffset();
            int length = buffer.limit() - 1 - idSize;
            JsonNode jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
            JsonSchema writerSchema = null;
            boolean hasReadSchema = readerSchema != null;
            // 在特定条件下，进行schema的校验和迁移
            if (hasReadSchema && latestCompatStrict && schemaAndVersion) {
                Pair<String, JsonSchema> pair = getWriteSchema(subject, topic, id);
                writerSchema = pair.getValue();
                subject = pair.getKey();
                List<Migration> migrations = getMigrations(subject, writerSchema, readerSchema);
                if (!migrations.isEmpty()) {
                    jsonNode = (JsonNode) executeMigrations(migrations, subject, topic, headers, jsonNode);
                }
            }
            // 处理读取和写入schema的匹配和验证
            if (hasReadSchema) {
                writerSchema = readerSchema;
            } else {
                writerSchema = ((JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id));
            }
            // 应用读规则
            if (writerSchema.ruleSet() != null && writerSchema.ruleSet().hasRules(RuleMode.READ)) {
                jsonNode = (JsonNode) executeRules(
                        subject, topic, headers, payload, RuleMode.READ, null, writerSchema, jsonNode
                );
            }
            // 验证JSON节点是否符合schema
            if (validate) {
                try {
                    writerSchema.validate(jsonNode);
                } catch (JsonProcessingException | ValidationException e) {
                    throw new SerializationException("JSON "
                            + jsonNode
                            + " does not match schema "
                            + writerSchema.canonicalString(), e);
                }
            }
            // 根据配置和schema类型将JSON节点转换为相应的Java对象
            Object value;
            if (type != null && !Object.class.equals(type)) {
                value = objectMapper.convertValue(jsonNode, type);
            } else {
                String typeName;
                if (writerSchema.rawSchema() instanceof CombinedSchema) {
                    typeName = getTypeName(writerSchema.rawSchema(), jsonNode);
                } else {
                    typeName = writerSchema.getString(typeProperty);
                }
                if (typeName != null) {
                    value = jsonNode != null
                            ? deriveType(jsonNode, typeName)
                            : deriveType(buffer, length, start, typeName);
                } else if (Object.class.equals(type)) {
                    value = objectMapper.convertValue(jsonNode, type);
                } else {
                    // 如果类型信息为空，则直接返回JSON节点
                    value = jsonNode;
                }
            }

            // 如果请求了schema和版本信息，则返回一个包含schema和对象的Pair
            if (schemaAndVersion) {
                return Pair.of(writerSchema, value);
            }
            return value;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error deserializing JSON message for id " + id, e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing JSON message for id " + id, e);
        } catch (RestClientException e) {
            throw toKafkaException(e, "Error retrieving JSON schema for id " + id);
        } finally {
            // 执行反序列化后的清理操作
            postOp(payload);
        }
    }

    /**
     * 根据给定的主题获取读取模式的JSON架构。
     *
     * @param subject 主题名称，用于在模式注册表中查找对应的架构。
     * @return JsonSchema 对象，表示与给定主题相关的读取模式架构。
     * @throws IOException         当发生I/O错误时抛出。
     * @throws RestClientException 当与服务器的通信出现问题时抛出。
     */
    private JsonSchema getReadSchema(String subject) throws IOException, RestClientException {
        JsonSchema readerSchema = null;
        // 使用预定义的架构ID来获取架构
        if (useSchemaId != -1) {
            readerSchema = ((JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, useSchemaId));
        } else if (metadata != null) {
            // 使用元数据来获取最新的架构
            readerSchema = (JsonSchema) getLatestWithMetadata(subject).getSchema();
        } else if (useLatestVersion) {
            // 获取主题的最新版本架构
            SchemaMetadata meta = schemaRegistry.getLatestSchemaMetadata(subject);
            readerSchema = new JsonSchema(meta.getSchema(), meta.getReferences(), Map.of(), meta.getMetadata(),
                    meta.getRuleSet(), meta.getVersion());
        }

        return readerSchema;
    }


    /**
     * 获取用于写入操作的 JSON Schema。
     *
     * @param subject 主题名称，标识一类 Schema。
     * @param topic   话题名称，通常与具体的事件或消息类型相关。
     * @param id      Schema 的 ID，用于从注册表中查找对应的 Schema。
     * @return 返回经过处理，适合用于写操作的 JSON Schema 实例。
     * @throws IOException         当与 Schema 注册表的通信发生错误时抛出。
     * @throws RestClientException 当调用注册表API发生错误时抛出。
     */
    private Pair<String, JsonSchema> getWriteSchema(String subject, String topic, Integer id) throws IOException, RestClientException {
        // 通过主题和ID从注册表获取原始的写作Schema
        JsonSchema writerSchema = ((JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id));
        // 根据主题、是否为键以及Schema信息调整主题名称
        subject = subjectName(topic, isKey, writerSchema);
        // 为反序列化准备Schema，可能涉及版本转换或调整
        writerSchema = schemaForDeserialize(id, writerSchema, subject, isKey);
        // 确定并获取最终使用的Schema版本号
        Integer version = schemaVersion(topic, isKey, id, subject, writerSchema, null);
        // 创建Schema的副本，并设置为确定使用的版本
        return Pair.of(subject, writerSchema.copy(version));
    }

    @Override
    public void close() {

    }
}
