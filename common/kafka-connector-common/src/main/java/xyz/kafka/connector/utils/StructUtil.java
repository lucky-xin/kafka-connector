package xyz.kafka.connector.utils;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.connect.schema.ConnectUnion;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverterConfig;
import xyz.kafka.serialization.json.JsonDataConfig;
import xyz.kafka.utils.Constants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.connect.avro.AvroData.GENERALIZED_TYPE_UNION_FIELD_PREFIX;
import static xyz.kafka.serialization.json.JsonData.JSON_TYPE_ONE_OF;

/**
 * StructUtil
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-08-14
 */
@SuppressWarnings("all")
public class StructUtil {

    /**
     * LogicalTypeConverter
     *
     * @author luchaoxin
     * @version V 1.0
     * @since 2023-06-20
     */
    public interface LogicalTypeConverter {

        /**
         * object to json node
         *
         * @param schema
         * @param value
         * @param config
         * @return
         */
        <T> T toJsonData(Schema schema, Object value, JsonDataConfig config);

        /**
         * json node to object
         *
         * @param schema
         * @param value
         * @return
         */
        Object toConnectData(Schema schema, Object value);
    }

    private static final Map<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

    private static final JsonNodeFactory JSON_NODE_FACTORY = new JsonNodeFactory(true);

    static {
        LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public <T> T toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof BigDecimal decimal)) {
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                }
                return switch (config.decimalFormat()) {
                    case NUMERIC -> (T) (decimal);
                    case BASE64 -> (T) (Decimal.fromLogical(schema, decimal));
                    default ->
                            throw new DataException("Unexpected " + JsonConverterConfig.DECIMAL_FORMAT_CONFIG + ": " + config.decimalFormat());
                };
            }

            @Override
            public Object toConnectData(final Schema schema, final Object value) {
                if (value instanceof Number n) {
                    return n;
                }
                byte[] b = null;
                if (value instanceof byte[] bytes) {
                    b = bytes;
                }

                if (value instanceof String s) {
                    b = s.getBytes(StandardCharsets.UTF_8);
                }
                if (b != null) {
                    try {
                        return Decimal.toLogical(schema, b);
                    } catch (Exception e) {
                        throw new DataException("Invalid bytes for Decimal field", e);
                    }
                }

                throw new DataException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value.getClass());
            }
        });

        LOGICAL_CONVERTERS.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof java.util.Date date)) {
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                }
                if (config.dateFormatter().isPresent()) {
                    return config.dateFormatter().get().format(Instant.ofEpochMilli(date.getTime()));
                }

                return org.apache.kafka.connect.data.Date.fromLogical(schema, date);
            }

            @Override
            public Object toConnectData(final Schema schema, final Object value) {
                if (value instanceof String s) {
                    LocalDate ld = LocalDate.parse(s, DateTimeFormatter.ISO_DATE);
                    return new java.util.Date(ld.atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
                }

                if (!(value instanceof Integer i)) {
                    throw new DataException("Invalid type for Date, underlying representation should be integer but was " + value.getClass());
                }
                return org.apache.kafka.connect.data.Date.toLogical(schema, i);
            }
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof java.util.Date date)) {
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                }
                if (config.timeFormatter().isPresent()) {
                    return config.timeFormatter().get().format(Instant.ofEpochMilli(date.getTime()));
                }

                return Time.fromLogical(schema, (java.util.Date) value);
            }

            @Override
            public Object toConnectData(final Schema schema, final Object value) {
                if (!(value instanceof Integer i)) {
                    throw new DataException("Invalid type for Time, underlying representation should be integer but was " + value.getClass());
                }
                return Time.toLogical(schema, i);
            }
        });

        LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof java.util.Date date)) {
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                }
                if (config.dateTimeFormatter().isPresent()) {
                    Instant instant = Instant.ofEpochMilli(date.getTime());
                    if (value instanceof java.sql.Timestamp t) {
                        instant = instant.plusNanos(t.getNanos());
                    }
                    return config.dateTimeFormatter().get().format(instant);
                }
                return Timestamp.fromLogical(schema, date);
            }

            @Override
            public Object toConnectData(final Schema schema, final Object value) {
                if (!(value instanceof Long l)) {
                    throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getClass());
                }
                return Timestamp.toLogical(schema, l);
            }
        });
        LOGICAL_CONVERTERS.put(Constants.LOGICAL_BIG_INTEGER_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof BigInteger b)) {
                    throw new DataException("Invalid type for BigInteger, expected Date but was " + value.getClass());
                }
                return b;
            }

            @Override
            public Object toConnectData(final Schema schema, final Object value) {
                if (!(value instanceof Integer i)) {
                    throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getClass());
                }
                return i;
            }
        });
    }

    @SuppressWarnings("all")
    public static <T> T fromConnectData(Schema schema, Object logicalValue, JsonDataConfig config) {
        if (logicalValue == null) {
            if (schema == null) {
                // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            }
            if (schema.defaultValue() != null && !config.ignoreDefaultForNullables()) {
                return fromConnectData(schema, schema.defaultValue(), config);
            }
            return null;
        }

        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null) {
                return logicalConverter.toJsonData(schema, logicalValue, config);
            }
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(logicalValue.getClass());
                if (schemaType == null) {
                    throw new DataException("Java class "
                            + logicalValue.getClass()
                            + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8 -> {
                    return (T) (Byte.class.cast(logicalValue));
                }
                case INT16 -> {
                    return (T) (Short.class.cast(logicalValue));
                }
                case INT32 -> {
                    return (T) (Integer.class.cast(logicalValue));
                }
                case INT64 -> {
                    return (T) (Long.class.cast(logicalValue));
                }
                case FLOAT32 -> {
                    return (T) (Float.class.cast(logicalValue));
                }
                case FLOAT64 -> {
                    return (T) (Double.class.cast(logicalValue));
                }
                case BOOLEAN -> {
                    return (T) (Boolean.class.cast(logicalValue));
                }
                case STRING -> {
                    return (T) (String.class.cast(logicalValue));
                }
                case BYTES -> {
                    if (logicalValue instanceof byte[] bytes) {
                        return (T) (bytes);
                    } else if (logicalValue instanceof ByteBuffer buffer) {
                        return (T) (buffer.array());
                    } else if (logicalValue instanceof BigDecimal bd) {
                        return (T) (Base64.getEncoder().encode(bd.toBigInteger().toByteArray()));
                    } else {
                        throw new DataException("Invalid type for bytes type: " + logicalValue.getClass());
                    }
                }
                case ARRAY -> {
                    Collection<?> collection = (Collection<?>) logicalValue;
                    List<Object> values = new ArrayList<>(collection.size());
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object fieldValue = fromConnectData(valueSchema, elem, config);
                        values.add(fieldValue);
                    }
                    return (T) (values);
                }
                case MAP -> {
                    Map<?, ?> map = (Map<?, ?>) logicalValue;
                    // If true, using string keys and JSON object; if false, using non-string keys and
                    // Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema()
                                .isOptional();
                    }
                    Map<Object, Object> obj = null;
                    List<Object> list = null;
                    if (objectMode) {
                        obj = new HashMap<>();
                    } else {
                        list = new ArrayList<>();
                    }
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object mapKey = fromConnectData(keySchema, entry.getKey(), config);
                        Object mapValue = fromConnectData(valueSchema, entry.getValue(), config);

                        if (objectMode) {
                            obj.put(mapKey.toString(), mapValue);
                        } else {
                            list.add(Map.of(mapKey.toString(), mapValue));
                        }
                    }
                    return objectMode ? (T) (obj) : (T) (list);
                }
                case STRUCT -> {
                    Struct struct = (Struct) logicalValue;
                    if (schema == null || !struct.schema().type().equals(schema.type())) {
                        throw new DataException("Mismatching schema.");
                    }
                    //This handles the inverting of a union which is held as a struct, where each field is
                    // one of the union types.
                    if (isUnionSchema(schema)) {
                        for (Field field : schema.fields()) {
                            Object object = config.ignoreDefaultForNullables()
                                    ? struct.getWithoutDefault(field.name()) : struct.get(field);
                            if (object != null) {
                                return fromConnectData(field.schema(), object, config);
                            }
                        }
                        return fromConnectData(schema, null, config);
                    } else {
                        Map<Object, Object> map = new HashMap<>(schema.fields().size());
                        for (Field field : schema.fields()) {
                            Object fieldValue = config.ignoreDefaultForNullables()
                                    ? struct.getWithoutDefault(field.name()) : struct.get(field);
                            Object jsonNode = fromConnectData(field.schema(), fieldValue, config);
                            if (jsonNode != null) {
                                map.put(field.name(), jsonNode);
                            }
                        }
                        return (T) (map);
                    }
                }
                default -> {
                }
            }

            throw new DataException("Couldn't convert value to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + logicalValue.getClass());
        }
    }

    @SuppressWarnings("all")
    public static <T> T toConnectData(Schema schema, Object orig) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (orig == null) {
                if (schema.defaultValue() != null) {
                    // any logical type conversions should already have been applied
                    return (T) schema.defaultValue();
                }
                if (schema.isOptional()) {
                    return null;
                }
                throw new DataException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            if (orig == null) {
                return null;
            }
            schemaType = ConnectSchema.schemaType(orig.getClass());
        }
        return switch (schemaType) {
            case BOOLEAN, INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, STRING -> (T) (orig);

            case BYTES -> {
                if (orig instanceof String s) {
                    yield ((T) (Base64.getDecoder().decode(s)));
                }
                if (orig instanceof byte[] s) {
                    yield (T) (s);
                }
                throw new DataException("Invalid bytes field: " + schema.name() + " source type is:"
                        + orig.getClass().getName());
            }

            case MAP -> {
                Schema keySchema = schema.keySchema();
                Schema valueSchema = schema.valueSchema();
                Map<Object, Object> result = new HashMap<>();
                if (orig instanceof Map<?, ?> map) {
                    if (keySchema.type() == Schema.Type.STRING && !keySchema.isOptional()) {
                        for (Map.Entry<?, ?> value : map.entrySet()) {
                            Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) value;
                            result.put(entry.getKey(), toConnectData(valueSchema, entry.getValue()));
                        }
                        yield (T) (result);
                    } else {
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            result.put(
                                    toConnectData(keySchema, entry.getKey()),
                                    toConnectData(valueSchema, entry.getValue())
                            );
                        }
                        yield (T) (result);
                    }
                }
                throw new DataException(
                        "Maps with string fields should be encoded as JSON objects, but found "
                                + orig.getClass());
            }

            case ARRAY -> {
                Schema elemSchema = schema.valueSchema();
                ArrayList<Object> result = new ArrayList<>();
                for (Object elem : ((Collection<Object>) orig)) {
                    result.add(toConnectData(elemSchema, elem));
                }
                yield (T) (result);
            }

            case STRUCT -> {
                if (isUnionSchema(schema)) {
                    boolean generalizedSumTypeSupport = ConnectUnion.isUnion(schema);
                    String prefix = generalizedSumTypeSupport
                            ? GENERALIZED_TYPE_UNION_FIELD_PREFIX
                            : JSON_TYPE_ONE_OF + ".field.";
                    int numMatchingProperties = -1;
                    Field matchingField = null;
                    for (Field field : schema.fields()) {
                        Schema fieldSchema = field.schema();
                        if (isInstanceOfSchemaTypeForSimpleSchema(fieldSchema, orig)) {
                            yield (T) (new Struct(schema.schema())
                                    .put(prefix + field.index(), toConnectData(fieldSchema, orig)));
                        } else {
                            int matching = matchStructSchema(fieldSchema, orig);
                            if (matching > numMatchingProperties) {
                                numMatchingProperties = matching;
                                matchingField = field;
                            }
                        }
                    }
                    if (matchingField != null) {
                        yield (T) (new Struct(schema.schema()).put(
                                prefix + matchingField.index(),
                                toConnectData(matchingField.schema(), orig)
                        ));
                    }
                    throw new DataException("Did not find matching oneof field for data");
                } else {
                    Map<String, Object> map = (Map<String, Object>) orig;
                    Struct result = new Struct(schema);
                    List<Field> fields = schema.fields();
                    for (Field field : schema.fields()) {
                        Object jsonNode = map.get(field.name());
                        Object fieldValue = toConnectData(field.schema(), jsonNode);
                        result.put(field.name(), fieldValue);
                    }
                    yield (T) (result);
                }
            }
            default -> throw new DataException("Structs should be encoded as " + schema.type() + " objects, but found "
                    + orig.getClass().getName());
        };
    }

    private static boolean isInstanceOfSchemaTypeForSimpleSchema(Schema fieldSchema, Object value) {
        return switch (fieldSchema.type()) {
            case INT8 -> value instanceof Byte;
            case INT16 -> value instanceof Short;
            case INT32 -> value instanceof Integer;
            case INT64 -> value instanceof Long;
            case FLOAT32 -> value instanceof Float;
            case FLOAT64 -> value instanceof Double;
            case BOOLEAN -> value instanceof Boolean;
            case STRING -> value instanceof String;
            case BYTES -> value instanceof byte[] || value instanceof BigDecimal;
            case ARRAY -> value instanceof Object[] || value instanceof Collection<?>;
            case MAP -> value instanceof Map<?, ?>;
            case STRUCT -> false;
            default -> throw new IllegalArgumentException("Unsupported type " + fieldSchema.type());
        };
    }

    @SuppressWarnings("unchecked")
    private static int matchStructSchema(Schema fieldSchema, Object value) {
        if (fieldSchema.type() != Schema.Type.STRUCT || !(value instanceof Map<?, ?>)) {
            return -1;
        }
        Set<String> schemaFields = fieldSchema.fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.toSet());
        Set<String> objectFields = new HashSet<>();
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
            objectFields.add(entry.getKey());
        }
        Set<String> intersectSet = new HashSet<>(schemaFields);
        intersectSet.retainAll(objectFields);
        return intersectSet.size();
    }

    private static boolean isUnionSchema(Schema schema) {
        return JSON_TYPE_ONE_OF.equals(schema.name()) || ConnectUnion.isUnion(schema);
    }

    public static <T> T tryGetValue(Struct struct, String field) {
        Schema schema = struct.schema();
        if (schema.field(field) == null) {
            return null;
        }
        return (T) struct.get(field);
    }

}
