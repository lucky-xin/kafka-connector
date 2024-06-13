package xyz.kafka.serialization.protobuf;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ProtobufSerializer
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class ProtobufSerializer<T extends Message>
        extends AbstractProtobufSerializer<T> implements Serializer<T> {

    private static final int DEFAULT_CACHE_CAPACITY = 1000;

    private boolean isKey;
    private final Cache<Descriptor, ProtobufSchema> schemaCache;

    /**
     * Constructor used by Kafka producer.
     */
    public ProtobufSerializer() {
        this.schemaCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .maximumSize(DEFAULT_CACHE_CAPACITY)
                .ticker(Ticker.systemTicker())
                .softValues()
                .build();
    }

    public ProtobufSerializer(SchemaRegistryClient client) {
        this.schemaRegistry = client;
        this.schemaCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .maximumSize(DEFAULT_CACHE_CAPACITY)
                .ticker(Ticker.systemTicker())
                .softValues()
                .build();
    }

    public ProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        this(client, props, DEFAULT_CACHE_CAPACITY);
    }

    public ProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props, int cacheCapacity) {
        this.schemaRegistry = client;
        configure(serializerConfig(props));
        this.schemaCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .maximumSize(cacheCapacity)
                .ticker(Ticker.systemTicker())
                .softValues()
                .build();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        configure(new ProtobufSerializerConfig(configs));
    }

    @Override
    public byte[] serialize(String topic, T t) {
        return serialize(topic, null, t);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (schemaRegistry == null) {
            throw new InvalidConfigurationException(
                    "SchemaRegistryClient not found. You need to configure the serializer "
                            + "or use serializer constructor with SchemaRegistryClient.");
        }
        if (data == null) {
            return new byte[0];
        }
        ProtobufSchema schema = schemaCache.get(data.getDescriptorForType(), k -> {
            ProtobufSchema s = ProtobufSchemaUtils.getSchema(data);
            try {
                // Ensure dependencies are resolved before caching
                boolean autoRegisterForDeps = autoRegisterSchema && !onlyLookupReferencesBySchema;
                boolean useLatestForDeps = useLatestVersion && !onlyLookupReferencesBySchema;
                return resolveDependencies(schemaRegistry, normalizeSchema, autoRegisterForDeps,
                        useLatestForDeps, latestCompatStrict, latestVersionsCache(),
                        skipKnownTypes, referenceSubjectNameStrategy, topic, isKey, s);
            } catch (IOException | RestClientException e) {
                throw new SerializationException("Error serializing Protobuf message", e);
            }
        });
        return serializeImpl(getSubjectName(topic, isKey, data, schema), topic, headers, schema, data);
    }

    @Override
    public void close() {
    }
}
