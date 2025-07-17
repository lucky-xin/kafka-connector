package xyz.kafka.connector.transforms;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.enums.BehaviorOnError;
import xyz.kafka.connector.recommenders.EnumRecommender;
import xyz.kafka.connector.utils.StructUtil;
import xyz.kafka.connector.validator.Validators;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.ssl.IgnoreClientCheckTrustManager;
import xyz.kafka.utils.StringUtil;

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
import java.util.ArrayList;
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
public class Embeddings<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(Embeddings.class);

    public static final String EMBEDDINGS_FIELDS = "fields";
    public static final String EMBEDDINGS_ENABLE_ALL_FIELD = "enable.all.field";
    public static final String EMBEDDINGS_BEHAVIOR_ON_ERROR = "behavior.on.error";
    public static final String EMBEDDINGS_ENDPOINT = "endpoint";
    public static final String EMBEDDINGS_INPUT_FIELD = "request.input.field";
    public static final String EMBEDDINGS_TIMEOUT_MS = "request.timeout.ms";
    public static final String EMBEDDINGS_JSON_PATH_IN_RESPONSE = "json.path.in.response";
    public static final String EMBEDDINGS_OUTPUT_FIELD = "output.field";
    public static final String EMBEDDINGS_TOPIC_TO_LABEL_MAPPER = "topic2label.mapper";

    public static final String EMBEDDINGS_HEADERS_PREFIX = "headers.";
    public static final String EMBEDDINGS_REQUEST_PARAMS_PREFIX = "params.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    EMBEDDINGS_ENABLE_ALL_FIELD,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    "embedding all field"
            ).define(
                    EMBEDDINGS_FIELDS,
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
                    EMBEDDINGS_TIMEOUT_MS,
                    ConfigDef.Type.LONG,
                    1000 * 10,
                    Validators.between(1, 1000 * 60 * 100),
                    ConfigDef.Importance.LOW,
                    "embeddings input field name."
            ).define(
                    EMBEDDINGS_OUTPUT_FIELD,
                    ConfigDef.Type.STRING,
                    "embeddings",
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.LOW,
                    "embeddings input field name."
            ).define(
                    EMBEDDINGS_JSON_PATH_IN_RESPONSE,
                    ConfigDef.Type.STRING,
                    "$.data[*].embedding",
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.LOW,
                    "embeddings json path in response."
            ).define(
                    EMBEDDINGS_BEHAVIOR_ON_ERROR,
                    ConfigDef.Type.STRING,
                    BehaviorOnError.LOG.name(),
                    new EnumRecommender<>(BehaviorOnError.class),
                    ConfigDef.Importance.LOW,
                    "behavior on error."
            ).define(
                    EMBEDDINGS_TOPIC_TO_LABEL_MAPPER,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    ConfigDef.Importance.LOW,
                    "behavior on error."
            );

    private HttpClient httpClient;
    private String endpoint;
    private String inputField;
    private String outputField;
    private long timeout;
    private List<String> fields;
    private Map<String, String> headers;
    private Map<String, String> params;
    private Map<String, String> topicToLabel;
    private JSONPath embeddingsJsonPath;
    private boolean embeddingAllField;
    private BehaviorOnError behaviorOnError;
    private JsonDataConfig jsonDataConfig;

    enum EmbeddingFormat {
        /**
         * double
         */
        DOUBLE,
        DECIMAL
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        endpoint = config.getString(EMBEDDINGS_ENDPOINT);
        embeddingAllField = config.getBoolean(EMBEDDINGS_ENABLE_ALL_FIELD);
        inputField = config.getString(EMBEDDINGS_INPUT_FIELD);
        timeout = config.getLong(EMBEDDINGS_TIMEOUT_MS);
        outputField = config.getString(EMBEDDINGS_OUTPUT_FIELD);
        embeddingsJsonPath = JSONPath.of(config.getString(EMBEDDINGS_JSON_PATH_IN_RESPONSE));
        fields = config.getList(EMBEDDINGS_FIELDS);
        headers = getWithPrefix(configs, EMBEDDINGS_HEADERS_PREFIX);
        params = getWithPrefix(configs, EMBEDDINGS_REQUEST_PARAMS_PREFIX);
        behaviorOnError = BehaviorOnError.valueOf(config.getString(EMBEDDINGS_BEHAVIOR_ON_ERROR));
        jsonDataConfig = new JsonDataConfig(configs);
        topicToLabel = config.getList(EMBEDDINGS_TOPIC_TO_LABEL_MAPPER)
                .stream()
                .map(s -> s.split(":"))
                .collect(Collectors.toMap(s -> s[0], s -> s[1]));
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
        if (!embeddingAllField && fields.isEmpty()) {
            throw new ConnectException("Either fields or embeddingAllField must be set.");
        }
    }

    @Override
    public R apply(R r) {
        Struct valueStruct = Requirements.requireStructOrNull(r.value(), r.topic());
        if (valueStruct == null || valueStruct.schema().fields().isEmpty()) {
            return r;
        }
        Schema newSchema = valueStruct.schema();
        Struct newValue = valueStruct;
        if (!embeddingAllField) {
            SchemaBuilder builder = SchemaBuilder.struct();
            Schema valueSchema = valueStruct.schema();
            for (String fieldName : fields) {
                Field field = valueSchema.field(fieldName);
                builder.field(fieldName, field.schema());
            }
            newSchema = builder.build();
            newValue = new Struct(newSchema);
            for (String fieldName : fields) {
                newValue.put(fieldName, valueStruct.get(fieldName));
            }
        }

        SchemaBuilder builder = SchemaBuilder.struct()
                .field(outputField, SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
                .field("properties", Schema.STRING_SCHEMA)
                .field("topic", Schema.STRING_SCHEMA)
                .field("label", Schema.STRING_SCHEMA);
        Struct keyStruct = Requirements.requireStructOrNull(r.key(), r.topic());
        Schema keySchema = keyStruct.schema();
        for (Field field : keySchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        Schema schema = builder.build();
        Struct value = new Struct(builder);
        for (Field field : keySchema.fields()) {
            value.put(field.name(), keyStruct.get(field));
        }
        try {
            Object val = StructUtil.fromConnectData(newSchema, newValue, jsonDataConfig);
            String jsonText = JSON.toJSONString(val);
            List<Number> embeddings = textsToEmbeddings(jsonText);
            value.put(outputField, new ArrayList<>(embeddings));
            value.put("properties", jsonText);
            value.put("topic", r.topic());
            value.put("label", topicToLabel.getOrDefault(r.topic(), r.topic()));
        } catch (Exception e) {
            switch (behaviorOnError) {
                case LOG -> log.error("Embeddings text error.", e);
                case FAIL -> throw new ConnectException("Embeddings text error", e);
                case IGNORE -> {

                }
                default -> {
                }
            }
            return null;
        }
        return r.newRecord(
                r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, value, r.timestamp(), r.headers()
        );
    }

    @SuppressWarnings("unchecked")
    private List<Number> textsToEmbeddings(String text) {
        try {
            Map<String, Object> body = new HashMap<>(this.params);
            body.put(inputField, List.of(text));
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.toJSONString(body)))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofMillis(timeout));
            headers.forEach(builder::header);
            String resultText = this.httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            ).body();
            JSONObject root = JSON.parseObject(
                    resultText,
                    // 可用于强制 double
                    JSONReader.Feature.UseDoubleForDecimals
            );
            return Optional.ofNullable((List<List<Number>>) embeddingsJsonPath.eval(root))
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

    @NotNull
    private static Map<String, String> getWithPrefix(Map<String, ?> props, String keyPrefix) {
        return props.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(keyPrefix))
                .collect(Collectors.toMap(
                        e -> e.getKey().replace(keyPrefix, ""),
                        t -> StringUtil.toString(t.getValue())
                ));
    }
}
