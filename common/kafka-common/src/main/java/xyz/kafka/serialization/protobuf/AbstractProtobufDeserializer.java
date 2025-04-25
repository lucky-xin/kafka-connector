package xyz.kafka.serialization.protobuf;

import cn.hutool.core.lang.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.Nullable;
import xyz.kafka.serialization.AbstractKafkaSchemaSerDer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * AbstractProtobufDeserializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public abstract class AbstractProtobufDeserializer<T extends Message> extends AbstractKafkaSchemaSerDer {

    protected Class<T> specificProtobufClass;
    protected Method parseMethod;
    protected boolean deriveType;

    protected AbstractProtobufDeserializer() {

    }

    /**
     * Sets properties for this deserializer without overriding the schema registry client itself.
     * Useful for testing, where a mock client is injected.
     */
    protected void configure(ProtobufDeserializerConfig config, Class<T> type) {
        super.configureClientProperties(config, new ProtobufSchemaProvider());
        try {
            this.specificProtobufClass = type;
            if (specificProtobufClass != null && !specificProtobufClass.equals(Object.class)) {
                this.parseMethod = specificProtobufClass.getDeclaredMethod("parseFrom", ByteBuffer.class);
            }
            this.deriveType = config.getBoolean(ProtobufDeserializerConfig.DERIVE_TYPE_CONFIG);
        } catch (Exception e) {
            throw new ConfigException("Class " + specificProtobufClass.getCanonicalName()
                    + " is not a valid protobuf message class", e);
        }
    }

    protected ProtobufDeserializerConfig deserializerConfig(Map<String, ?> props) {
        try {
            return new ProtobufDeserializerConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
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
        return (T) deserialize(false, null, isKey, null, payload);
    }

    /**
     * 反序列化函数，用于将字节数据转换为对象。该函数根据不同的配置和上下文，灵活地使用protobuf schema进行反序列化。
     * 支持包含schema和版本信息以及不包含这些信息的反序列化场景。
     *
     * @param includeSchemaAndVersion 是否在结果中包含schema和版本信息。
     * @param topic                   消息的主题。
     * @param isKey                   消息是否为键。
     * @param headers                 消息的头部信息。
     * @param payload                 消息的负载数据，即需要被反序列化的字节数据。
     * @return 根据配置和上下文，可能返回反序列化后的对象，或者包含schema和版本信息的对象对。
     * @throws SerializationException        当序列化过程发生错误时抛出。
     * @throws InvalidConfigurationException 当配置无效时抛出。
     */
    protected Object deserialize(boolean includeSchemaAndVersion, String topic, boolean isKey, Headers headers,
                                 byte[] payload) throws SerializationException, InvalidConfigurationException {
        // 检查payload是否为空
        if (payload == null || payload.length == 0) {
            return null;
        }
        int id = -1;
        try {
            // 初始化ByteBuffer并读取schema ID
            ByteBuffer buffer = getByteBuffer(payload);
            id = buffer.getInt();
            // 根据配置确定使用的subject名称
            String subject = strategyUsesSchema(isKey)
                    ? getContextName(topic) : subjectName(topic, isKey, null);
            // 根据不同的策略获取reader schema
            ProtobufSchema readerSchema = getReadSchema(subject);
            // 从buffer中读取消息索引
            MessageIndexes indexes = MessageIndexes.readFrom(buffer);

            // 初始化writer schema并处理schema兼容性和版本
            ProtobufSchema writerSchema = null;
            int length = buffer.limit() - 1 - idSize;
            int start = buffer.position() + buffer.arrayOffset();
            Object message = null;
            boolean hashReadSchema = readerSchema != null;
            if (hashReadSchema && latestCompatStrict && includeSchemaAndVersion) {
                Pair<String, ProtobufSchema> pair = getWriteSchema(subject, topic, id);
                writerSchema = pair.getValue();
                subject = pair.getKey();
                writerSchema = schemaWithName(writerSchema, writerSchema.toMessageName(indexes));
                List<Migration> migrations = getMigrations(subject, writerSchema, readerSchema);
                message = DynamicMessage.parseFrom(
                        writerSchema.toDescriptor(), new ByteArrayInputStream(buffer.array(), start, length)
                );
                if (!migrations.isEmpty()) {
                    message = executeMigrations(migrations, subject, topic, headers, message);
                    message = readerSchema.fromJson((JsonNode) message);
                }
            }
            // 处理未读取schema的情况
            if (hashReadSchema) {
                writerSchema = readerSchema;
            } else {
                writerSchema = ((ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id));
            }
            // 设置writer schema的名称
            String name = writerSchema.toMessageName(indexes);
            writerSchema = schemaWithName(writerSchema, name);
            // 执行读取规则
            if (writerSchema.ruleSet() != null && writerSchema.ruleSet().hasRules(RuleMode.READ)) {
                message = executeRules(
                        subject, topic, headers, payload, RuleMode.READ, null, writerSchema, message
                );
            }
            // 如果message不为空，重新包装buffer
            if (message != null) {
                buffer = ByteBuffer.wrap(((Message) message).toByteArray());
                length = buffer.limit();
                start = 0;
            }

            // 根据配置反序列化消息
            Object value;
            if (parseMethod != null) {
                try {
                    value = parseMethod.invoke(null, buffer);
                } catch (Exception e) {
                    throw new ConfigException("Not a valid protobuf builder", e);
                }
            } else if (deriveType) {
                value = deriveType(buffer, writerSchema);
            } else {
                Descriptor descriptor = writerSchema.toDescriptor();
                if (descriptor == null) {
                    throw new SerializationException("Could not find descriptor with name " + writerSchema.name());
                }
                value = DynamicMessage.parseFrom(descriptor,
                        new ByteArrayInputStream(buffer.array(), start, length)
                );
            }

            // 如果需要，返回包含schema和版本信息的结果
            if (includeSchemaAndVersion) {
                return Pair.of(writerSchema, value);
            }

            return value;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error deserializing Protobuf message for id " + id, e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing Protobuf message for id " + id, e);
        } catch (RestClientException e) {
            throw toKafkaException(e, "Error retrieving Protobuf schema for id " + id);
        } finally {
            // 执行反序列化后的操作
            postOp(payload);
        }
    }

    private @Nullable ProtobufSchema getReadSchema(String subject) throws IOException, RestClientException {
        ProtobufSchema readerSchema = null;
        if (useSchemaId != -1) {
            readerSchema = (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, useSchemaId);
        } else if (metadata != null) {
            readerSchema = (ProtobufSchema) getLatestWithMetadata(subject).getSchema();
        } else if (useLatestVersion) {
            SchemaMetadata meta = schemaRegistry.getLatestSchemaMetadata(subject);
            readerSchema = new ProtobufSchema(meta.getSchema(), meta.getReferences(), Map.of(), meta.getMetadata(),
                    meta.getRuleSet(), meta.getVersion(), null);
        }
        return readerSchema;
    }

    private Pair<String, ProtobufSchema> getWriteSchema(String subject, String topic, Integer id) throws RestClientException, IOException {
        ProtobufSchema writerSchema = (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
        subject = subjectName(topic, isKey, writerSchema);
        writerSchema = schemaForDeserialize(id, writerSchema, subject, isKey);
        Integer version = schemaVersion(topic, isKey, id, subject, writerSchema, null);
        return Pair.of(subject, writerSchema.copy(version));
    }


    private ProtobufSchema schemaWithName(ProtobufSchema schema, String name) {
        return schema.copy(name);
    }

    private Object deriveType(ByteBuffer buffer, ProtobufSchema schema) {
        String clsName = schema.fullName();
        if (clsName == null) {
            throw new SerializationException("If `derive.type` is true, then either "
                    + "`java_outer_classname` or `java_multiple_files = true` must be set "
                    + "in the Protobuf schema");
        }
        try {
            Class<?> cls = Class.forName(clsName);
            Method m = cls.getDeclaredMethod("parseFrom", ByteBuffer.class);
            return m.invoke(null, buffer);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + clsName + " could not be found.");
        } catch (NoSuchMethodException e) {
            throw new SerializationException("Class " + clsName
                    + " is not a valid protobuf message class", e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SerializationException("Not a valid protobuf builder");
        }
    }

    private String subjectName(String topic, boolean isKey, ProtobufSchema schemaFromRegistry) {
        return isDeprecatedSubjectNameStrategy(isKey)
                ? null
                : getSubjectName(topic, isKey, null, schemaFromRegistry);
    }

    private Integer schemaVersion(
            String topic, boolean isKey, int id, String subject, ProtobufSchema schema, Object value
    ) {
        try {
            if (isDeprecatedSubjectNameStrategy(isKey)) {
                subject = getSubjectName(topic, isKey, value, schema);
            }
            ProtobufSchema subjectSchema = (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
            return schemaRegistry.getVersion(subject, subjectSchema);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ProtobufSchema schemaForDeserialize(
            int id, ProtobufSchema schemaFromRegistry, String subject, boolean isKey
    ) throws IOException, RestClientException {
        return isDeprecatedSubjectNameStrategy(isKey)
                ? ProtobufSchemaUtils.copyOf(schemaFromRegistry)
                : (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
    }

    @SuppressWarnings({"unchecked"})
    protected Pair<ProtobufSchema, Object> deserializeWithSchemaAndVersion(
            String topic, boolean isKey, Headers headers, byte[] payload
    ) throws SerializationException {
        return (Pair<ProtobufSchema, Object>) deserialize(true, topic, isKey, headers, payload);
    }

}
