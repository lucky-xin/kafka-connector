package xyz.kafka.serialization.protobuf;

import com.google.protobuf.Message;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import xyz.kafka.serialization.AbstractKafkaSchemaSerDer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * AbstractProtobufSerializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public abstract class AbstractProtobufSerializer<T extends Message> extends AbstractKafkaSchemaSerDer {
    protected boolean onlyLookupReferencesBySchema;
    protected String schemaFormat;
    protected boolean skipKnownTypes;
    protected ReferenceSubjectNameStrategy referenceSubjectNameStrategy;

    protected void configure(ProtobufSerializerConfig config) {
        configureClientProperties(config, new ProtobufSchemaProvider());
        this.onlyLookupReferencesBySchema = config.onlyLookupReferencesBySchema();
        this.schemaFormat = config.getSchemaFormat();
        this.skipKnownTypes = config.skipKnownTypes();
        this.referenceSubjectNameStrategy = config.referenceSubjectNameStrategyInstance();
    }

    protected ProtobufSerializerConfig serializerConfig(Map<String, ?> props) {
        try {
            return new ProtobufSerializerConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
    }

    /**
     * 序列化实现方法。
     * 该方法用于将指定的对象序列化为字节数组，准备进行消息传输或存储。
     * 注意：此方法专门处理Protobuf格式的数据。
     *
     * @param subject 主题，标识数据的类型或分类。
     * @param topic 消息的主题，用于消息队列或订阅。
     * @param headers 消息的头部信息，可以包含额外的元数据。
     * @param schema 对象的schema定义，用于指导序列化过程。
     * @param object 需要被序列化的对象实例。
     * @return 返回序列化后的字节数组。
     * @throws SerializationException 当序列化过程发生错误时抛出。
     * @throws InvalidConfigurationException 当配置不合法时抛出。
     */
    @SuppressWarnings({"unchecked"})
    protected byte[] serializeImpl(
            String subject, String topic, Headers headers, ProtobufSchema schema, T object
    ) throws SerializationException, InvalidConfigurationException {
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema registration and return a null value in Kafka, instead
        // of an encoded null.
        if (object == null) {
            return new byte[0];
        }
        String restClientErrorMsg = "";
        try {
            boolean autoRegisterForDeps = autoRegisterSchema && !onlyLookupReferencesBySchema;
            boolean useLatestForDeps = useLatestVersion && !onlyLookupReferencesBySchema;
            schema = resolveDependencies(schemaRegistry, normalizeSchema, autoRegisterForDeps,
                    useLatestForDeps, latestCompatStrict, latestVersionsCache(),
                    skipKnownTypes, referenceSubjectNameStrategy, topic, isKey, schema);
            int id;
            if (useSchemaId >= 0) {
                restClientErrorMsg = "Error retrieving schema ID";
                if (schemaFormat != null) {
                    String formatted = schema.formattedString(schemaFormat);
                    schema = schema.copyWithSchema(formatted);
                }
                schema = (ProtobufSchema)
                        lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
                id = schemaRegistry.getId(subject, schema);
            } else if (metadata != null) {
                restClientErrorMsg = "Error retrieving latest with metadata '" + metadata + "'";
                schema = (ProtobufSchema) getLatestWithMetadata(subject).getSchema();
                id = schemaRegistry.getId(subject, schema);
            } else if (useLatestVersion) {
                restClientErrorMsg = "Error retrieving latest version: ";
                schema = (ProtobufSchema) lookupLatestVersion(subject, schema, latestCompatStrict).getSchema();
                id = schemaRegistry.getId(subject, schema);
            } else {
                restClientErrorMsg = "Error retrieving Protobuf schema: ";
                id = schemaRegistry.getId(subject, schema, normalizeSchema);
            }
            object = (T) executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, object);
            MessageIndexes indexes =
                    schema.toMessageIndexes(object.getDescriptorForType().getFullName(), normalizeSchema);
            byte[] indexesBytes = indexes.toByteArray();
            byte[] dataBytes = object.toByteArray();
            int size = idSize + 1 + indexesBytes.length + dataBytes.length;
            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.put(MAGIC_BYTE)
                    .putInt(id)
                    .put(indexesBytes)
                    .put(dataBytes);
            return buffer.array();

        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error serializing Protobuf message", e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error serializing Protobuf message", e);
        } catch (RestClientException e) {
            throw toKafkaException(e, restClientErrorMsg + schema);
        } finally {
            postOp(object);
        }
    }

    /**
     * Resolve schema dependencies recursively.
     *
     * @param schemaRegistry     schema registry client
     * @param autoRegisterSchema whether to automatically register schemas
     * @param useLatestVersion   whether to use the latest subject version for serialization
     * @param latestCompatStrict whether to check that the latest subject version is backward
     *                           compatible with the schema of the object
     * @param latestVersions     an optional cache of latest subject versions, may be null
     * @param strategy           the strategy for determining the subject name for a reference
     * @param topic              the topic
     * @param isKey              whether the object is the record key
     * @param schema             the schema
     * @return the schema with resolved dependencies
     */
    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        return resolveDependencies(
                schemaRegistry,
                autoRegisterSchema,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                true,
                strategy,
                topic,
                isKey,
                schema);
    }

    /**
     * Resolve schema dependencies recursively.
     *
     * @param schemaRegistry     schema registry client
     * @param autoRegisterSchema whether to automatically register schemas
     * @param useLatestVersion   whether to use the latest subject version for serialization
     * @param latestCompatStrict whether to check that the latest subject version is backward
     *                           compatible with the schema of the object
     * @param latestVersions     an optional cache of latest subject versions, may be null
     * @param skipKnownTypes     whether to skip known types when resolving schema dependencies
     * @param strategy           the strategy for determining the subject name for a reference
     * @param topic              the topic
     * @param isKey              whether the object is the record key
     * @param schema             the schema
     * @return the schema with resolved dependencies
     */
    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        return resolveDependencies(
                schemaRegistry,
                false,
                autoRegisterSchema,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                skipKnownTypes,
                strategy,
                topic,
                isKey,
                schema
        );
    }

    /**
     * Resolve schema dependencies recursively.
     *
     * @param schemaRegistry     schema registry client
     * @param normalizeSchema    whether to normalized the schema
     * @param autoRegisterSchema whether to automatically register schemas
     * @param useLatestVersion   whether to use the latest subject version for serialization
     * @param latestCompatStrict whether to check that the latest subject version is backward
     *                           compatible with the schema of the object
     * @param latestVersions     an optional cache of latest subject versions, may be null
     * @param skipKnownTypes     whether to skip known types when resolving schema dependencies
     * @param strategy           the strategy for determining the subject name for a reference
     * @param topic              the topic
     * @param isKey              whether the object is the record key
     * @param schema             the schema
     * @return the schema with resolved dependencies
     */
    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean normalizeSchema,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        if (schema.dependencies().isEmpty() || !schema.references().isEmpty()) {
            // Dependencies already resolved
            return schema;
        }
        Schema s = resolveDependencies(schemaRegistry,
                normalizeSchema,
                autoRegisterSchema,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                skipKnownTypes,
                strategy,
                topic,
                isKey,
                null,
                schema.rawSchema(),
                schema.dependencies()
        );
        return schema.copy(s.getReferences());
    }

    private static Schema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean normalizeSchema,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            String name,
            ProtoFileElement protoFileElement,
            Map<String, ProtoFileElement> dependencies
    ) throws IOException, RestClientException {
        List<SchemaReference> references = new ArrayList<>();
        for (String dep : protoFileElement.getImports()) {
            if (skipKnownTypes && ProtobufSchema.knownTypes().contains(dep)) {
                continue;
            }
            Schema subschema = resolveDependencies(schemaRegistry,
                    normalizeSchema,
                    autoRegisterSchema,
                    useLatestVersion,
                    latestCompatStrict,
                    latestVersions,
                    skipKnownTypes,
                    strategy,
                    topic,
                    isKey,
                    dep,
                    dependencies.get(dep),
                    dependencies
            );
            references.add(new SchemaReference(dep, subschema.getSubject(), subschema.getVersion()));
        }
        for (String dep : protoFileElement.getPublicImports()) {
            if (skipKnownTypes && ProtobufSchema.knownTypes().contains(dep)) {
                continue;
            }
            Schema subschema = resolveDependencies(schemaRegistry,
                    normalizeSchema,
                    autoRegisterSchema,
                    useLatestVersion,
                    latestCompatStrict,
                    latestVersions,
                    skipKnownTypes,
                    strategy,
                    topic,
                    isKey,
                    dep,
                    dependencies.get(dep),
                    dependencies
            );
            references.add(new SchemaReference(dep, subschema.getSubject(), subschema.getVersion()));
        }
        ProtobufSchema schema = new ProtobufSchema(protoFileElement, references, dependencies);
        Integer id = null;
        Integer version = null;
        String subject = name != null ? strategy.subjectName(name, topic, isKey, schema) : null;
        if (subject != null) {
            if (autoRegisterSchema) {
                id = schemaRegistry.register(subject, schema, normalizeSchema);
                version = schemaRegistry.getVersion(subject, schema, normalizeSchema);
            } else if (useLatestVersion) {
                schema = (ProtobufSchema) lookupLatestVersion(
                        schemaRegistry, subject, schema, latestVersions, latestCompatStrict).getSchema();
                id = schemaRegistry.getId(subject, schema);
                version = schemaRegistry.getVersion(subject, schema);
            } else {
                id = schemaRegistry.getId(subject, schema, normalizeSchema);
                version = schemaRegistry.getVersion(subject, schema, normalizeSchema);
            }
        }
        return new Schema(
                subject,
                version,
                id,
                schema
        );
    }
}
