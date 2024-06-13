package xyz.kafka.serialization;

import cn.hutool.core.text.CharSequenceUtil;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.serialization.strategy.JsonIdStrategy;
import xyz.kafka.serialization.strategy.ProtobufIdStrategy;
import xyz.kafka.serialization.strategy.TopicNameStrategy;
import xyz.kafka.utils.ConfigUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.MISSING_SCHEMA_CACHE_TTL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;

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
            originals.putIfAbsent(MISSING_ID_CACHE_TTL_CONFIG, 7200L);
            originals.putIfAbsent(MISSING_SCHEMA_CACHE_TTL_CONFIG, 7200L);
            originals.putIfAbsent(CLIENT_NAMESPACE + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            String userInfo = System.getenv("SCHEMA_REGISTRY_CLIENT_USER_INFO");
            if (CharSequenceUtil.isNotEmpty(userInfo)) {
                originals.putIfAbsent(SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
                originals.putIfAbsent(SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryClientConfig.USER_INFO_CONFIG, userInfo);
            }
            String registrySvcEndpoint = System.getenv("KAFKA_SCHEMA_REGISTRY_SVC_ENDPOINT");
            if (CharSequenceUtil.isNotBlank(registrySvcEndpoint)) {
                originals.putIfAbsent(SCHEMA_REGISTRY_URL_CONFIG, registrySvcEndpoint);
            }

            RestService restService = new RestService(config.getSchemaRegistryUrls());
            Map<String, String> httpHeaders = Optional.ofNullable(config.requestHeaders())
                    .filter(t -> !t.isEmpty())
                    .orElseGet(() -> ConfigUtil.getRestHeaders("SCHEMA_REGISTRY_CLIENT_REST_HEADERS"));
            this.schemaRegistry = new CachedSchemaRegistryClient(
                    restService,
                    config.getMaxSchemasPerSubject(),
                    List.of(provider),
                    originals,
                    httpHeaders
            );
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
