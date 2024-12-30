package xyz.kafka.serialization;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.factory.SchemaRegistryClientFactory;
import xyz.kafka.serialization.strategy.JsonIdStrategy;
import xyz.kafka.serialization.strategy.ProtobufIdStrategy;
import xyz.kafka.serialization.strategy.TopicNameStrategy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;

/**
 * 序列化反序列化抽象类
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-10
 */
public abstract class AbstractKafkaSchemaSerDer extends AbstractKafkaSchemaSerDe {
    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaSchemaSerDer.class);

    protected boolean latestCompatStrict;
    protected int useSchemaId;
    protected boolean autoRegisterSchema;

    protected boolean normalizeSchema;
    protected boolean idCompatStrict;

    @Override
    protected void configureClientProperties(AbstractKafkaSchemaSerDeConfig config, SchemaProvider provider) {
        this.latestCompatStrict = config.getLatestCompatibilityStrict();
        Map<String, Object> originals = new HashMap<>(config.originalsWithPrefix(""));
        if (schemaRegistry == null) {
            Map<String, String> collect = config.originalsWithPrefix(CLIENT_NAMESPACE)
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> Objects.toString(e.getValue())
                    ));
            this.schemaRegistry = SchemaRegistryClientFactory.builder()
                    .originals(collect)
                    .build()
                    .create();
        }
        super.configureClientProperties(config, provider);
        this.idCompatStrict = config.getIdCompatibilityStrict();
        this.normalizeSchema = config.normalizeSchema();
        this.useSchemaId = config.useSchemaId();
        this.autoRegisterSchema = config.autoRegisterSchema();

        if (!originals.containsKey(KEY_SUBJECT_NAME_STRATEGY)) {
            this.keySubjectNameStrategy = provider instanceof JsonSchemaProvider
                    ? new JsonIdStrategy() : new ProtobufIdStrategy();
        }
        if (!originals.containsKey(VALUE_SUBJECT_NAME_STRATEGY)) {
            this.valueSubjectNameStrategy = new TopicNameStrategy();
        }

        if (this.keySubjectNameStrategy instanceof Configurable c) {
            c.configure(config.originalsWithPrefix("key.subject.name.strategy."));
        }
        if (this.valueSubjectNameStrategy instanceof Configurable c) {
            c.configure(config.originalsWithPrefix("value.subject.name.strategy."));
        }
        this.useLatestVersion = config.useLatestVersion();

    }


    public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
        return schemaRegistry.getLatestSchemaMetadata(subject);
    }

    public SchemaRegistryClient schemaRegistry() {
        return schemaRegistry;
    }

    @Override
    public void close() {
        try {
            super.close();
            this.schemaRegistry.close();
        } catch (IOException e) {
            log.error("Close schema registry client error", e);
        }
    }
}
