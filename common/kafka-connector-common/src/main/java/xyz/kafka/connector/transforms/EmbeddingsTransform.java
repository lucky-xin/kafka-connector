package xyz.kafka.connector.transforms;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.enums.BehaviorOnError;
import xyz.kafka.connector.recommenders.EnumRecommender;
import xyz.kafka.connector.validator.Validators;
import xyz.kafka.ssl.IgnoreClientCheckTrustManager;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 向量转换
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class EmbeddingsTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(EmbeddingsTransform.class);

    public static final String FIELDS = "fields";
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
    public static final String EMBEDDINGS_ENDPOINT = "embeddings.endpoint";
    public static final String EMBEDDINGS_INPUT_FIELD = "embeddings.request.input.field";
    public static final String EMBEDDINGS_REQUEST_PARAMS = "embeddings.request.params";
    public static final String EMBEDDINGS_JSON_PATH_IN_RESPONSE = "embeddings.json.path.in.response";
    public static final String OUTPUT_FIELD = "output.field";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    FIELDS,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    ConfigDef.Importance.MEDIUM,
                    "Fields to convert."
            ).define(
                    EMBEDDINGS_ENDPOINT,
                    ConfigDef.Type.STRING,
                    "https://dashscope.aliyuncs.com/compatible-mode/v1",
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.LOW,
                    "embeddings service endpoint."
            ).define(
                    EMBEDDINGS_INPUT_FIELD,
                    ConfigDef.Type.STRING,
                    "input",
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.LOW,
                    "embeddings input field name."
            ).define(
                    OUTPUT_FIELD,
                    ConfigDef.Type.STRING,
                    "embeddings",
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.LOW,
                    "embeddings input field name."
            ).define(
                    EMBEDDINGS_REQUEST_PARAMS,
                    ConfigDef.Type.LIST,
                    "",
                    ConfigDef.Importance.LOW,
                    "embeddings request params."
            ).define(
                    EMBEDDINGS_JSON_PATH_IN_RESPONSE,
                    ConfigDef.Type.STRING,
                    "$.data[*].embedding",
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.LOW,
                    "embeddings json path in response."
            ).define(
                    BEHAVIOR_ON_ERROR,
                    ConfigDef.Type.STRING,
                    BehaviorOnError.LOG.name(),
                    new EnumRecommender<>(BehaviorOnError.class),
                    ConfigDef.Importance.LOW,
                    "behavior on error."
            );

    private HttpClient httpClient;
    private String endpoint;
    private String inputField;
    private String outputField;
    private List<String> fieldList;
    private Map<String, Object> params;
    private JSONPath embeddingsJsonPath;
    private BehaviorOnError behaviorOnError;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        endpoint = config.getString(EMBEDDINGS_ENDPOINT);
        inputField = config.getString(EMBEDDINGS_INPUT_FIELD);
        outputField = config.getString(OUTPUT_FIELD);
        embeddingsJsonPath = JSONPath.of(config.getString(EMBEDDINGS_JSON_PATH_IN_RESPONSE));
        fieldList = config.getList(FIELDS);
        params = config.getList(EMBEDDINGS_REQUEST_PARAMS)
                .stream()
                .map(s -> s.split("="))
                .collect(Collectors.toMap(
                        s -> s[0],
                        s -> s[1],
                        (k1, k2) -> k1
                ));
        behaviorOnError = BehaviorOnError.valueOf(config.getString(BEHAVIOR_ON_ERROR));
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[]{new IgnoreClientCheckTrustManager(false)}, new SecureRandom());
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .version(endpoint.startsWith("https") ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1)
                    .sslContext(context)
                    .build();
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public R apply(R r) {
        Struct struct = Requirements.requireStructOrNull(r.value(), r.topic());
        if (struct == null) {
            return null;
        }
        Map<String, Object> values = new HashMap<>(struct.schema().fields().size());
        for (String field : fieldList) {
            Object fieldValue = struct.get(field);
            if (fieldValue == null) {
                continue;
            }
            values.put(field, fieldValue);
        }
        Struct updatedValue = new Struct(
                SchemaBuilder.struct()
                        .field(outputField, SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
                        .build()
        );
        try {
            String jsonText = JSON.toJSONString(values);
            List<Double> embeddings = textsToEmbeddings(jsonText);
            updatedValue.put(outputField, embeddings);
        } catch (Exception e) {
            switch (behaviorOnError) {
                case LOG -> log.error("Embeddings text error.", e);
                case FAIL -> throw new ConnectException("Embeddings text error", e);
                case IGNORE -> {

                }
                default -> {
                }
            }
        }
        return r.newRecord(
                r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), updatedValue.schema(),
                updatedValue, r.timestamp(), r.headers()
        );
    }

    @SuppressWarnings("unchecked")
    private List<Double> textsToEmbeddings(String text) {
        try {
            Map<String, Object> body = new HashMap<>(this.params);
            body.put(inputField, List.of(text));

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.toJSONString(body)))
                    .header("Content-Type", "application/json")
                    .build();
            String resultText = this.httpClient.send(
                    req,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            ).body();
            JSONObject root = JSON.parseObject(resultText);
            return Optional.ofNullable((List<List<Double>>) embeddingsJsonPath.eval(root))
                    .map(l -> l.get(0))
                    .orElseGet(Collections::emptyList);
        } catch (IOException | InterruptedException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // ooo
    }
}
