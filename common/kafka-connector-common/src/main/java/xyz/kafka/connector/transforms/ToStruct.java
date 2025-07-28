package xyz.kafka.connector.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.validator.Validators;
import xyz.kafka.serialization.json.JsonData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/**
 * Transformation that converts a complex value into a JSON string.
 * Can be used as a workaround for <a href="https://github.com/confluentinc/kafka-connect-jdbc/issues/46">...</a>
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class ToStruct<T extends ConnectRecord<T>> extends AbstractTransformation<T>
        implements KeyOrValueTransformation<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ToStruct.class);

    public static final String FIELD_PAIRS = "field.pairs";
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";

    private Map<String, String> fieldPairs;

    private BehaviorOnError behavior;

    private ObjectMapper mapper;

    public enum BehaviorOnError {
        /**
         * 忽略
         */
        IGNORE, LOG, FAIL, DROP;
    }

    protected ToStruct() {
        super(new ConfigDef()
                .define(FIELD_PAIRS,
                        ConfigDef.Type.LIST,
                        NO_DEFAULT_VALUE,
                        Validators.nonEmptyList(),
                        ConfigDef.Importance.HIGH,
                        "Name pair of the source field and target field whose value will be serialized to JSON"
                ).define(BEHAVIOR_ON_ERROR,
                        ConfigDef.Type.STRING,
                        BehaviorOnError.LOG.name(),
                        Validators.oneOfUppercase(BehaviorOnError.class),
                        ConfigDef.Importance.HIGH,
                        "behavior on error, one of:LOG,IGNORE,THROW"
                )
        );
    }

    @Override
    public void configure(Map<String, ?> configs, AbstractConfig config) {
        this.fieldPairs = config.getList(FIELD_PAIRS)
                .stream()
                .map(t -> t.split(":"))
                .collect(Collectors.toMap(
                        t -> t[0].strip(),
                        t -> t[1].strip()
                ));
        this.behavior = BehaviorOnError.valueOf(config.getString(BEHAVIOR_ON_ERROR));
        this.mapper = new ObjectMapper();
    }

    @Override
    public T apply(T t) {
        Struct struct = Requirements.requireStructOrNull(value(t), t.topic());
        if (struct == null) {
            return null;
        }
        Schema schema = schema(t);
        List<Field> fields = schema.fields();
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Map<String, Object> values = new HashMap<>(this.fieldPairs.size());
        fields.forEach(f -> {
            if (fieldPairs.containsKey(f.name())) {
                return;
            }
            builder.field(f.name(), f.schema());
            values.put(f.name(), struct.get(f.name()));
        });

        for (Map.Entry<String, String> e : this.fieldPairs.entrySet()) {
            String source = e.getKey();
            String target = e.getValue();
            Field field = schema.field(source);
            if (field == null) {
                continue;
            }
            Optional<SchemaAndValue> op = tryToStruct(struct, source, t.topic());
            boolean label = behavior != BehaviorOnError.DROP
                    && behavior != BehaviorOnError.LOG
                    && op.isEmpty();
            if (label) {
                builder.field(source, field.schema());
                values.put(target, struct.get(source));
                continue;
            }
            op.ifPresent(sav -> {
                builder.field(target, sav.schema());
                values.put(target, sav.value());
            });
        }
        Schema newSchema = builder.schema();
        Struct newStruct = new Struct(newSchema);
        values.forEach(newStruct::put);
        return newRecord(t, newStruct, newSchema);
    }

    private Optional<SchemaAndValue> tryToStruct(Struct struct, String source, String topic) {
        Object obj = struct.get(source);
        if (obj instanceof String s) {
            try {
                JsonNode node = mapper.readTree(s);

                Schema schema = xyz.kafka.connector.utils.SchemaUtil.inferSchema(node);
                Object connectData = JsonData.toConnectData(schema, node);
                return Optional.of(new SchemaAndValue(schema, connectData));
            } catch (Exception ex) {
                switch (behavior) {
                    case LOG -> LOG.error("convert field [" + source + "] to struct failed,value:" + s, ex);
                    case FAIL -> throw new ConnectException(ex);
                    case IGNORE, DROP -> {
                    }
                    default -> {

                    }
                }
            }
        } else if (obj instanceof Struct s) {
            return Optional.of(new SchemaAndValue(s.schema(), s));
        } else if (obj instanceof Map<?, ?> s) {
            JsonNode node = mapper.valueToTree(s);
            Schema schema = xyz.kafka.connector.utils.SchemaUtil.inferSchema(node);
            Object connectData = JsonData.toConnectData(schema, node);
            return Optional.of(new SchemaAndValue(schema, connectData));
        }
        return Optional.empty();
    }

    public static class Key<T extends ConnectRecord<T>> extends ToStruct<T>
            implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends ToStruct<T>
            implements KeyOrValueTransformation.Value<T> {
    }
}
