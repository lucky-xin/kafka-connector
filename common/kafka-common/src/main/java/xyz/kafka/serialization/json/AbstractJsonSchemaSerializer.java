package xyz.kafka.serialization.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.everit.json.schema.ValidationException;
import xyz.kafka.serialization.AbstractKafkaSchemaSerDer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * AbstractJsonSchemaSerializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public abstract class AbstractJsonSchemaSerializer<T> extends AbstractKafkaSchemaSerDer {

    protected ObjectMapper objectMapper = Jackson.newObjectMapper();
    protected SpecificationVersion specVersion;
    protected boolean oneofForNullables;
    protected boolean validate;

    protected JsonData jsonData;

    protected void configure(Map<String, ?> orig) {
        JsonSchemaSerializerConfig config = new JsonSchemaSerializerConfig(orig);
        configureClientProperties(config, new JsonSchemaProvider());
        boolean prettyPrint = config.getBoolean(JsonSchemaSerializerConfig.JSON_INDENT_OUTPUT);
        boolean writeDatesAsIso8601 = config.getBoolean(JsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601);
        Boolean failUnknownProperties = config.getBoolean(JsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
        this.objectMapper.configure(SerializationFeature.INDENT_OUTPUT, prettyPrint);
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, !writeDatesAsIso8601);
        this.objectMapper.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, failUnknownProperties);
        this.objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        this.specVersion = SpecificationVersion.get(config.getString(JsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION));
        this.oneofForNullables = config.getBoolean(JsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES);
        this.validate = config.getBoolean(JsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA);
        if (this.jsonData == null) {
            this.jsonData = new JsonData(new JsonDataConfig(orig));
        }
    }

    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    @SuppressWarnings({"unchecked"})
    protected byte[] serializeImpl(
            String subject,
            String topic,
            Headers headers,
            T object
    ) throws SerializationException, InvalidConfigurationException {
        if (schemaRegistry == null) {
            throw new InvalidConfigurationException(
                    "SchemaRegistryClient not found. You need to configure the serializer "
                            + "or use serializer constructor with SchemaRegistryClient.");
        }
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema registration and return a null value in Kafka, instead
        // of an encoded null.
        if (object == null) {
            return new byte[0];
        }
        String restClientErrorMsg = "Error retrieving latest version: ";
        JsonSchema schema = null;
        try {
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
            schema = new JsonSchema(schemaMetadata.getSchema());
            int id = schemaMetadata.getId();
            if (useSchemaId >= 0) {
                restClientErrorMsg = "Error retrieving schema ID";
                schema = (JsonSchema) lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
                id = schemaRegistry.getId(subject, schema);
            } else if (metadata != null) {
                restClientErrorMsg = "Error retrieving latest with metadata '" + metadata + "'";
                schema = (JsonSchema) getLatestWithMetadata(subject);
                id = schemaRegistry.getId(subject, schema);
            } else if (!useLatestVersion) {
                restClientErrorMsg = "Error retrieving JSON schema: ";
                id = schemaRegistry.getId(subject, schema, normalizeSchema);
            }
            object = (T) executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, object);
            if (validate) {
                validateJson(object, schema);
            }
            byte[] dataBytes = objectMapper.writeValueAsBytes(object);
            int size = idSize + 1 + dataBytes.length;
            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.put(MAGIC_BYTE)
                    .putInt(id)
                    .put(dataBytes);
            return buffer.array();
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error serializing JSON message", e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error serializing JSON message", e);
        } catch (RestClientException e) {
            throw toKafkaException(e, restClientErrorMsg + schema);
        } finally {
            postOp(object);
        }
    }

    protected void validateJson(T object,
                                JsonSchema schema)
            throws SerializationException {
        try {
            JsonNode jsonNode = objectMapper.convertValue(object, JsonNode.class);
            schema.validate(jsonNode);
        } catch (JsonProcessingException e) {
            throw new SerializationException("JSON "
                    + object
                    + " does not match schema "
                    + schema.canonicalString(), e);
        } catch (ValidationException e) {
            throw new SerializationException("Validation error in JSON "
                    + object
                    + ", Error report:\n"
                    + e.toJSON().toString(2), e);
        }
    }

    @Override
    public void close() {
    }
}
