package xyz.kafka.factory;

import cn.hutool.core.text.CharSequenceUtil;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import lombok.Builder;
import org.apache.commons.collections.MapUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.REQUEST_HEADER_PREFIX;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;

/**
 * SchemaRegistryClientUtils
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-10-30
 */
@Builder
public class SchemaRegistryClientFactory {

    private Map<String, String> originals;

    public CachedSchemaRegistryClient create() {
        String userInfo = System.getenv("SCHEMA_REGISTRY_CLIENT_USER_INFO");
        if (CharSequenceUtil.isNotEmpty(userInfo)) {
            originals.putIfAbsent(CLIENT_NAMESPACE + BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            originals.putIfAbsent(CLIENT_NAMESPACE + USER_INFO_CONFIG, userInfo);
        }
        String registrySvcEndpoint = System.getenv("KAFKA_SCHEMA_REGISTRY_SVC_ENDPOINT");
        if (CharSequenceUtil.isNotBlank(registrySvcEndpoint)) {
            originals.putIfAbsent(SCHEMA_REGISTRY_URL_CONFIG, registrySvcEndpoint);
        }
        Map<String, String> headers = originals.entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(CLIENT_NAMESPACE + REQUEST_HEADER_PREFIX))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> Objects.toString(entry.getValue())
                ));
        originals.putIfAbsent(CLIENT_NAMESPACE + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        originals.putIfAbsent(CLIENT_NAMESPACE + MAX_SCHEMAS_PER_SUBJECT_CONFIG, "1000");
        RestService restService = new RestService(originals.get(SCHEMA_REGISTRY_URL_CONFIG));
        CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
                restService,
                MapUtils.getInteger(originals, CLIENT_NAMESPACE + MAX_SCHEMAS_PER_SUBJECT_CONFIG),
                List.of(new JsonSchemaProvider(), new ProtobufSchemaProvider(), new AvroSchemaProvider()),
                originals,
                headers
        );
        try {
            SSLContext context = SSLContext.getInstance("SSL");
            context.init(null, new TrustManager[]{new LTSTrustmanager(false)}, new SecureRandom());
            SSLSocketFactory socketFactory = context.getSocketFactory();
            restService.setSslSocketFactory(socketFactory);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return schemaRegistry;
    }

    private record LTSTrustmanager(boolean checkServerValidity) implements X509TrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] certificates, final String authType) {
            // document why this method is empty
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certificates, final String authType) throws CertificateException {
            if (this.checkServerValidity) {
                for (X509Certificate certificate : certificates) {
                    certificate.checkValidity();
                }
            }

        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

}
