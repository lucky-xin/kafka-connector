package xyz.kafka.serialization.json;

import cn.hutool.core.map.MapUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import xyz.kafka.constants.ConnectConstants;
import io.confluent.connect.schema.AbstractDataConfig;
import io.confluent.connect.schema.ConnectEnum;
import io.confluent.connect.schema.ConnectUnion;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * JsonData
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
@SuppressWarnings("all")
public class JsonData {
    public static final String NAMESPACE = "io.confluent.connect.json";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String CONNECT_TYPE_PROP = "connect.type";
    public static final String CONNECT_VERSION_PROP = "connect.version";
    public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
    public static final String CONNECT_INDEX_PROP = "connect.index";
    public static final String CONNECT_TYPE_INT8 = "int8";
    public static final String CONNECT_TYPE_INT16 = "int16";
    public static final String CONNECT_TYPE_INT32 = "int32";
    public static final String CONNECT_TYPE_INT64 = "int64";
    public static final String CONNECT_TYPE_FLOAT32 = "float32";
    public static final String CONNECT_TYPE_FLOAT64 = "float64";
    public static final String CONNECT_TYPE_BYTES = "bytes";
    public static final String CONNECT_TYPE_MAP = "map";
    public static final String DEFAULT_ID_PREFIX = "#id";
    public static final String JSON_ID_PROP = NAMESPACE + ".Id";
    public static final String JSON_TYPE_ENUM = NAMESPACE + ".Enum";
    public static final String JSON_TYPE_ONE_OF = NAMESPACE + ".OneOf";

    public static final String GENERALIZED_TYPE_UNION = ConnectUnion.LOGICAL_PARAMETER;
    public static final String GENERALIZED_TYPE_ENUM = ConnectEnum.LOGICAL_PARAMETER;
    public static final String GENERALIZED_TYPE_UNION_PREFIX = "connect_union_";
    public static final String GENERALIZED_TYPE_UNION_FIELD_PREFIX =
            GENERALIZED_TYPE_UNION_PREFIX + "field_";
    private static final JsonNodeFactory JSON_NODE_FACTORY = new JsonNodeFactory(true);

    private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper();

    private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .map(t -> "true".equals(t))
                        .orElse((Boolean) schema.defaultValue());
            }
            return value.booleanValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .filter(t -> t.length() == 1)
                        .map(t -> Byte.parseByte(t))
                        .orElse((Byte) schema.defaultValue());
            }
            return (byte) value.shortValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .map(t -> Short.parseShort(t))
                        .orElse((Short) schema.defaultValue());
            }
            return value.shortValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .map(t -> Integer.parseInt(t))
                        .orElse((Integer) schema.defaultValue());
            }
            return value.intValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .map(t -> Long.parseLong(t))
                        .orElse((Long) schema.defaultValue());
            }
            return value.longValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .map(t -> Float.parseFloat(t))
                        .orElse((Float) schema.defaultValue());
            }
            return value.floatValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value) -> {
            if (value instanceof TextNode tn) {
                return Optional.ofNullable(tn.asText())
                        .map(t -> Double.parseDouble(t))
                        .orElse((Double) schema.defaultValue());
            }
            return value.doubleValue();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (schema, value) -> {
            try {
                Object o = value.binaryValue();
                if (o == null) {
                    o = value.decimalValue();  // decimal logical type
                }
                return o;
            } catch (IOException e) {
                throw new DataException("Invalid bytes field", e);
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value) -> value.textValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, (schema, value) -> {
            Schema elemSchema = schema == null ? null : schema.valueSchema();
            ArrayList<Object> result = new ArrayList<>();
            for (JsonNode elem : value) {
                result.add(toConnectData(elemSchema, elem));
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, (schema, value) -> {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();

            Map<Object, Object> result = new HashMap<>();
            if (schema == null || (keySchema.type() == Schema.Type.STRING && !keySchema.isOptional())) {
                if (!value.isObject()) {
                    throw new DataException(
                            "Maps with string fields should be encoded as JSON objects, but found "
                                    + value.getNodeType());
                }
                Iterator<Entry<String, JsonNode>> fieldIt = value.fields();
                while (fieldIt.hasNext()) {
                    Entry<String, JsonNode> entry = fieldIt.next();
                    result.put(entry.getKey(), toConnectData(valueSchema, entry.getValue()));
                }
            } else {
                if (!value.isArray()) {
                    throw new DataException(
                            "Maps with non-string fields should be encoded as JSON array of objects, but "
                                    + "found "
                                    + value.getNodeType());
                }
                for (JsonNode entry : value) {
                    if (!entry.isObject()) {
                        throw new DataException("Found invalid map entry instead of object: "
                                + entry.getNodeType());
                    }
                    if (entry.size() != 2) {
                        throw new DataException("Found invalid map entry, expected length 2 but found :" + entry
                                .size());
                    }
                    result.put(toConnectData(keySchema, entry.get(KEY_FIELD)),
                            toConnectData(valueSchema, entry.get(VALUE_FIELD))
                    );
                }
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, (schema, value) -> {
            if (isUnionSchema(schema)) {
                boolean generalizedSumTypeSupport = ConnectUnion.isUnion(schema);
                String fieldNamePrefix = generalizedSumTypeSupport
                        ? GENERALIZED_TYPE_UNION_FIELD_PREFIX
                        : JSON_TYPE_ONE_OF + ".field.";
                int numMatchingProperties = -1;
                Field matchingField = null;
                for (Field field : schema.fields()) {
                    Schema fieldSchema = field.schema();

                    if (isInstanceOfSchemaTypeForSimpleSchema(fieldSchema, value)) {
                        return new Struct(schema.schema()).put(fieldNamePrefix + field.index(),
                                toConnectData(fieldSchema, value)
                        );
                    } else {
                        int matching = matchStructSchema(fieldSchema, value);
                        if (matching > numMatchingProperties) {
                            numMatchingProperties = matching;
                            matchingField = field;
                        }
                    }
                }
                if (matchingField != null) {
                    return new Struct(schema.schema()).put(
                            fieldNamePrefix + matchingField.index(),
                            toConnectData(matchingField.schema(), value)
                    );
                }
                throw new DataException("Did not find matching oneof field for data");
            } else {
                if (!value.isObject()) {
                    throw new DataException("Structs should be encoded as JSON objects, but found "
                            + value.getNodeType());
                }
                Struct result = new Struct(schema);
                for (Field field : schema.fields()) {
                    JsonNode jsonNode = value.get(field.name());
                    Object fieldValue = toConnectData(field.schema(), jsonNode);
                    result.put(field, fieldValue);
                }
                return result;
            }
        });
    }

    private static boolean isInstanceOfSchemaTypeForSimpleSchema(Schema fieldSchema, JsonNode value) {
        return switch (fieldSchema.type()) {
            case INT8, INT16, INT32, INT64 -> value.isIntegralNumber();
            case FLOAT32, FLOAT64 -> value.isNumber();
            case BOOLEAN -> value.isBoolean();
            case STRING -> value.isTextual();
            case BYTES -> value.isBinary() || value.isBigDecimal();
            case ARRAY -> value.isArray();
            case MAP -> value.isObject() || value.isArray();
            case STRUCT -> false;
            default -> throw new IllegalArgumentException("Unsupported type " + fieldSchema.type());
        };
    }

    private static int matchStructSchema(Schema fieldSchema, JsonNode value) {
        if (fieldSchema.type() != Schema.Type.STRUCT || !value.isObject()) {
            return -1;
        }
        Set<String> schemaFields = fieldSchema.fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.toSet());
        Set<String> objectFields = new HashSet<>();
        for (Iterator<Entry<String, JsonNode>> iter = value.fields(); iter.hasNext(); ) {
            objectFields.add(iter.next().getKey());
        }
        Set<String> intersectSet = new HashSet<>(schemaFields);
        intersectSet.retainAll(objectFields);
        return intersectSet.size();
    }

    // Convert values in Kafka Connect form into their logical types. These logical converters are
    // discovered by logical type
    // names specified in the field
    private static final Map<String, LogicalTypeConverter> LOGICAL_CONVERTERS =
            LogicalTypeConverterHolder.getLogicalConverters();

    private final JsonDataConfig config;
    private final Map<Schema, JsonSchema> fromConnectSchemaCache;
    private final Map<JsonSchema, Schema> toConnectSchemaCache;
    private final boolean generalizedSumTypeSupport;

    public JsonData() {
        this(new JsonDataConfig.Builder().with(
                AbstractDataConfig.SCHEMAS_CACHE_SIZE_CONFIG,
                AbstractDataConfig.SCHEMAS_CACHE_SIZE_DEFAULT
        ).build());
    }

    public JsonData(JsonDataConfig jsonDataConfig) {
        this.config = jsonDataConfig;
        fromConnectSchemaCache = new BoundedConcurrentHashMap<>(jsonDataConfig.schemaCacheSize());
        toConnectSchemaCache = new BoundedConcurrentHashMap<>(jsonDataConfig.schemaCacheSize());
        generalizedSumTypeSupport = jsonDataConfig.isGeneralizedSumTypeSupport();
    }

    /**
     * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object,
     * returning both the schema
     * and the converted object.
     */
    public JsonNode fromConnectData(Schema schema, Object logicalValue, boolean binaryToJson) {
        if (logicalValue == null) {
            if (schema == null) {
                // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            }
            if (schema.defaultValue() != null && !config.ignoreDefaultForNullables()) {
                return fromConnectData(schema, schema.defaultValue(), binaryToJson);
            }
            if (schema.isOptional()) {
                return JSON_NODE_FACTORY.nullNode();
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
                    // Use shortValue to create a ShortNode, otherwise an IntNode will be created
                    return JSON_NODE_FACTORY.numberNode(((Byte) logicalValue).shortValue());
                }
                case INT16 -> {
                    return JSON_NODE_FACTORY.numberNode((Short) logicalValue);
                }
                case INT32 -> {
                    return JSON_NODE_FACTORY.numberNode((Integer) logicalValue);
                }
                case INT64 -> {
                    return JSON_NODE_FACTORY.numberNode((Long) logicalValue);
                }
                case FLOAT32 -> {
                    return JSON_NODE_FACTORY.numberNode((Float) logicalValue);
                }
                case FLOAT64 -> {
                    return JSON_NODE_FACTORY.numberNode((Double) logicalValue);
                }
                case BOOLEAN -> {
                    return JSON_NODE_FACTORY.booleanNode((Boolean) logicalValue);
                }
                case STRING -> {
                    CharSequence charSeq = (CharSequence) logicalValue;
                    return JSON_NODE_FACTORY.textNode(charSeq.toString());
                }
                case BYTES -> {
                    if (logicalValue instanceof byte[] bytes) {
                        if (binaryToJson) {
                            return OBJECT_MAPPER.readTree(bytes);
                        }
                        return JSON_NODE_FACTORY.binaryNode(bytes);
                    } else if (logicalValue instanceof ByteBuffer buffer) {
                        if (binaryToJson) {
                            return OBJECT_MAPPER.readTree(buffer.array());
                        }
                        return JSON_NODE_FACTORY.binaryNode(buffer.array());
                    } else if (logicalValue instanceof BigDecimal bd) {
                        return JSON_NODE_FACTORY.numberNode(bd);
                    } else {
                        throw new DataException("Invalid type for bytes type: " + logicalValue.getClass());
                    }
                }
                case ARRAY -> {
                    Collection<?> collection = (Collection<?>) logicalValue;
                    ArrayNode list = JSON_NODE_FACTORY.arrayNode();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode fieldValue = fromConnectData(valueSchema, elem, false);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP -> {
                    Map<?, ?> map = (Map<?, ?>) logicalValue;
                    // If true, using string keys and JSON object; if false, using non-string keys and
                    // Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema()
                                .isOptional();
                    }
                    ObjectNode obj = null;
                    ArrayNode list = null;
                    if (objectMode) {
                        obj = JSON_NODE_FACTORY.objectNode();
                    } else {
                        list = JSON_NODE_FACTORY.arrayNode();
                    }
                    for (Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode mapKey = fromConnectData(keySchema, entry.getKey(), false);
                        JsonNode mapValue = fromConnectData(valueSchema, entry.getValue(), false);

                        if (objectMode) {
                            obj.set(mapKey.asText(), mapValue);
                        } else {
                            ObjectNode o = JSON_NODE_FACTORY.objectNode();
                            o.set(KEY_FIELD, mapKey);
                            o.set(VALUE_FIELD, mapValue);
                            list.add(o);
                        }
                    }
                    return objectMode ? obj : list;
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
                                return fromConnectData(field.schema(), object, false);
                            }
                        }
                        return fromConnectData(schema, null, false);
                    } else {
                        ObjectNode obj = JSON_NODE_FACTORY.objectNode();
                        for (Field field : schema.fields()) {
                            Object fieldValue = config.ignoreDefaultForNullables()
                                    ? struct.getWithoutDefault(field.name()) : struct.get(field);
                            JsonNode jsonNode = fromConnectData(field.schema(), fieldValue, false);
                            if (jsonNode != null) {
                                obj.set(field.name(), jsonNode);
                            }
                        }
                        return obj;
                    }
                }
                default -> {
                }
            }

            throw new DataException("Couldn't convert value to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + logicalValue.getClass());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public SchemaAndValue toConnectData(JsonSchema jsonSchema, JsonNode value) {
        return this.toConnectData(jsonSchema, value, (Integer) null);
    }

    /**
     * 将给定的 JSON Schema 和值转换为 connector 数据格式。
     *
     * @param jsonSchema JsonSchema 对象，表示待转换的 JSON Schema。
     * @param value JsonNode 对象，表示待转换的值。
     * @param version Integer 类型，表示 JSON Schema 的版本号。可选参数，用于指定转换时的 Schema 版本。
     * @return SchemaAndValue 对象，包含转换后的 Schema 和值。如果输入的值为 null，则返回 null。
     */
    public SchemaAndValue toConnectData(JsonSchema jsonSchema, JsonNode value, Integer version) {
        // 当值为 null 时，直接返回 null
        if (value == null) {
            return null;
        } else {
            // 创建转换上下文
            ToConnectContext toConnectContext = new ToConnectContext();
            // 根据 JSON Schema 转换为 Connect Schema
            Schema schema = toConnectSchema(toConnectContext, jsonSchema.rawSchema(), version);
            // 使用转换后的 Schema 和值创建 SchemaAndValue 对象并返回
            return new SchemaAndValue(schema, toConnectData(schema, value));
        }
    }

    /**
     * 将 JSON 数据转换为对应的connector数据对象。
     *
     * @param schema 用于描述数据结构的模式对象。如果为 null，则会尝试从 JSON 节点推断模式。
     * @param jsonValue 要转换的 JSON 节点。
     * @return 转换后的连接数据对象。可能的类型包括原始类型、复杂类型或 null。
     * @throws DataException 如果遇到无效的 null 值或未知的模式类型时抛出。
     */
    public static Object toConnectData(Schema schema, JsonNode jsonValue) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            // 当 schema 不为 null 且 jsonValue 为 null 时的处理逻辑
            if (jsonValue == null || jsonValue.isNull()) {
                // 检查是否有默认值并返回
                if (schema.defaultValue() != null) {
                    return schema.defaultValue();
                }
                // 当 jsonValue 为 null 且 schema 标记为可选时返回 null
                if (jsonValue == null || schema.isOptional()) {
                    return null;
                }
                // 当 jsonValue 为无效的 null 值且字段为必需字段时抛出异常
                throw new DataException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            // 当 schema 为 null 时根据 jsonValue 的类型推断 schemaType
            if (jsonValue == null) {
                return null;
            }
            switch (jsonValue.getNodeType()) {
                case NULL -> {
                    return null;
                }
                case BOOLEAN -> schemaType = Schema.Type.BOOLEAN;
                case NUMBER -> {
                    if (jsonValue.isIntegralNumber()) {
                        schemaType = Schema.Type.INT64;
                    } else {
                        schemaType = Schema.Type.FLOAT64;
                    }
                }
                case ARRAY -> schemaType = Schema.Type.ARRAY;
                case OBJECT -> schemaType = Schema.Type.MAP;
                case STRING -> schemaType = Schema.Type.STRING;
                case BINARY -> schemaType = Schema.Type.BYTES;
                default -> schemaType = null;
            }
        }

        // 检查逻辑类型转换器是否存在并进行转换
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null) {
                return logicalConverter.toConnectData(schema, jsonValue);
            }
        }
        // 根据 schema 类型获取对应的转换器并执行转换
        JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null) {
            // 当找不到对应的转换器时抛出异常
            throw new DataException("Unknown schema type: " + schemaType);
        }
        return typeConverter.convert(schema, jsonValue);
    }

    /**
     * 根据 connector 的 schema 创建 JsonSchema 对象。
     *
     * @param schema Connect 的 Schema 对象，不可为 null。
     * @return 对应的 JsonSchema 对象。如果输入的 schema 为 null，则返回 null。
     */
    public JsonSchema fromConnectSchema(Schema schema) {
        // 检查输入的 schema 是否为 null
        if (schema == null) {
            return null;
        }
        // 尝试从缓存中获取已转换的 JsonSchema
        JsonSchema cachedSchema = fromConnectSchemaCache.get(schema);
        if (cachedSchema != null) {
            // 如果缓存中存在，则直接返回缓存中的 JsonSchema
            return cachedSchema;
        }
        // 创建转换上下文
        FromConnectContext ctx = new FromConnectContext();
        // 调用 rawSchemaFromConnectSchema 方法转换 Schema，并创建新的 JsonSchema 对象
        JsonSchema resultSchema = new JsonSchema(rawSchemaFromConnectSchema(ctx, schema));
        // 将新转换的 JsonSchema 存入缓存
        fromConnectSchemaCache.put(schema, resultSchema);
        // 返回新转换的 JsonSchema
        return resultSchema;
    }


    private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
            FromConnectContext ctx, Schema schema) {
        return rawSchemaFromConnectSchema(ctx, schema, null);
    }

    private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
            FromConnectContext ctx, Schema schema, Integer index) {
        return rawSchemaFromConnectSchema(ctx, schema, index, false);
    }

    private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
            FromConnectContext ctx, Schema schema, Integer index, boolean ignoreOptional
    ) {
        if (schema == null) {
            return null;
        }

        String id = null;
        if (schema.parameters() != null && schema.parameters().containsKey(JSON_ID_PROP)) {
            id = schema.parameters().get(JSON_ID_PROP);
            ctx.add(id);
        }

        org.everit.json.schema.Schema.Builder builder;
        Map<String, Object> unprocessedProps = new HashMap<>();
        switch (schema.type()) {
            case INT8 -> {
                builder = NumberSchema.builder().requiresNumber(true).requiresInteger(true);
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT8);
            }
            case INT16 -> {
                builder = NumberSchema.builder().requiresNumber(true).requiresInteger(true);
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT16);
            }
            case INT32 -> {
                builder = NumberSchema.builder().requiresNumber(true).requiresInteger(true);
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32);
            }
            case INT64 -> {
                builder = NumberSchema.builder().requiresNumber(true).requiresInteger(true);
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64);
            }
            case FLOAT32 -> {
                builder = NumberSchema.builder().requiresNumber(true).requiresInteger(false);
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT32);
            }
            case FLOAT64 -> {
                builder = NumberSchema.builder().requiresNumber(true).requiresInteger(false);
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT64);
            }
            case BOOLEAN -> builder = BooleanSchema.builder();
            case STRING -> {
                if (schema.parameters() != null
                        && (schema.parameters().containsKey(GENERALIZED_TYPE_ENUM)
                        || schema.parameters().containsKey(JSON_TYPE_ENUM))) {
                    EnumSchema.Builder enumBuilder = EnumSchema.builder();
                    String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : JSON_TYPE_ENUM;
                    for (Entry<String, String> entry : schema.parameters().entrySet()) {
                        if (entry.getKey().startsWith(paramName + ".")) {
                            String enumSymbol = entry.getKey().substring(paramName.length() + 1);
                            enumBuilder.possibleValue(enumSymbol);
                        }
                    }
                    builder = enumBuilder;
                } else {
                    builder = StringSchema.builder();
                }
            }
            case BYTES -> {
                builder = Decimal.LOGICAL_NAME.equals(schema.name())
                        ? NumberSchema.builder()
                        : StringSchema.builder();
                unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);
            }
            case ARRAY -> {
                Schema arrayValueSchema = schema.valueSchema();
                String refId = null;
                if (arrayValueSchema.parameters() != null
                        && arrayValueSchema.parameters().containsKey(JSON_ID_PROP)) {
                    refId = arrayValueSchema.parameters().get(JSON_ID_PROP);
                }
                org.everit.json.schema.Schema itemsSchema;
                if (ctx.contains(refId)) {
                    itemsSchema = ReferenceSchema.builder().refValue(refId).build();
                } else {
                    itemsSchema = rawSchemaFromConnectSchema(ctx, arrayValueSchema);
                }
                builder = ArraySchema.builder().allItemSchema(itemsSchema);
            }
            case MAP -> {
                // JSON Schema only supports string keys
                if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
                    org.everit.json.schema.Schema valueSchema =
                            rawSchemaFromConnectSchema(ctx, schema.valueSchema());
                    builder = ObjectSchema.builder().schemaOfAdditionalProperties(valueSchema);
                    unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
                } else {
                    ObjectSchema.Builder entryBuilder = ObjectSchema.builder();
                    org.everit.json.schema.Schema keySchema =
                            rawSchemaFromConnectSchema(ctx, schema.keySchema(), 0);
                    org.everit.json.schema.Schema valueSchema =
                            rawSchemaFromConnectSchema(ctx, schema.valueSchema(), 1);
                    entryBuilder.addPropertySchema(KEY_FIELD, keySchema);
                    entryBuilder.addPropertySchema(VALUE_FIELD, valueSchema);
                    builder = ArraySchema.builder().allItemSchema(entryBuilder.build());
                    unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
                }
            }
            case STRUCT -> {
                if (isUnionSchema(schema)) {
                    CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
                    combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
                    if (schema.isOptional()) {
                        combinedBuilder.subschema(NullSchema.INSTANCE);
                    }
                    for (Field field : schema.fields()) {
                        combinedBuilder.subschema(rawSchemaFromConnectSchema(ctx, nonOptional(field.schema()),
                                field.index(),
                                true
                        ));
                    }
                    builder = combinedBuilder;
                } else if (schema.isOptional()) {
                    CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
                    combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
                    combinedBuilder.subschema(NullSchema.INSTANCE);
                    combinedBuilder.subschema(rawSchemaFromConnectSchema(ctx, nonOptional(schema)));
                    builder = combinedBuilder;
                } else {
                    ObjectSchema.Builder objectBuilder = ObjectSchema.builder();
                    for (Field field : schema.fields()) {
                        Schema fieldSchema = field.schema();
                        String fieldRefId = null;
                        if (fieldSchema.parameters() != null
                                && fieldSchema.parameters().containsKey(JSON_ID_PROP)) {
                            fieldRefId = fieldSchema.parameters().get(JSON_ID_PROP);
                        }
                        org.everit.json.schema.Schema jsonSchema;
                        if (ctx.contains(fieldRefId)) {
                            jsonSchema = ReferenceSchema.builder().refValue(fieldRefId).build();
                        } else {
                            jsonSchema = rawSchemaFromConnectSchema(ctx, fieldSchema, field.index());
                        }
                        objectBuilder.addPropertySchema(field.name(), jsonSchema);
                    }
                    if (!config.allowAdditionalProperties()) {
                        objectBuilder.additionalProperties(false);
                    }
                    builder = objectBuilder;
                }
            }
            default -> throw new IllegalArgumentException("Unsupported type " + schema.type());
        }

        if (!(builder instanceof CombinedSchema.Builder)) {
            if (schema.name() != null) {
                builder.title(schema.name());
            }
            if (schema.version() != null) {
                unprocessedProps.put(CONNECT_VERSION_PROP, schema.version());
            }
            if (schema.doc() != null) {
                builder.description(schema.doc());
            }
            if (schema.parameters() != null) {
                Map<String, String> parameters = schema.parameters()
                        .entrySet()
                        .stream()
                        .filter(e -> !e.getKey().startsWith(NAMESPACE))
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                if (!parameters.isEmpty()) {
                    unprocessedProps.put(CONNECT_PARAMETERS_PROP, parameters);
                }
            }
            if (schema.defaultValue() != null) {
                builder.defaultValue(fromConnectData(schema, schema.defaultValue(), false));
            }

            if (!ignoreOptional && (schema.isOptional())) {
                CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
                combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
                combinedBuilder.subschema(NullSchema.INSTANCE);
                combinedBuilder.subschema(builder.unprocessedProperties(unprocessedProps).build());
                if (index != null) {
                    combinedBuilder.unprocessedProperties(Collections.singletonMap(CONNECT_INDEX_PROP,
                            index
                    ));
                }
                builder = combinedBuilder;
                unprocessedProps = new HashMap<>();

            }
        }
        if (id != null) {
            builder.id(id);
        }
        if (index != null) {
            unprocessedProps.put(CONNECT_INDEX_PROP, index);
        }
        return builder.unprocessedProperties(unprocessedProps).build();
    }

    private static Schema nonOptional(Schema schema) {
        return new ConnectSchema(schema.type(),
                false,
                schema.defaultValue(),
                schema.name(),
                schema.version(),
                schema.doc(),
                schema.parameters(),
                fields(schema),
                keySchema(schema),
                valueSchema(schema)
        );
    }

    private static List<Field> fields(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.STRUCT.equals(type)) {
            return schema.fields();
        } else {
            return Collections.emptyList();
        }
    }

    private static Schema keySchema(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.MAP.equals(type)) {
            return schema.keySchema();
        } else {
            return null;
        }
    }

    private static Schema valueSchema(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.MAP.equals(type) || Schema.Type.ARRAY.equals(type)) {
            return schema.valueSchema();
        } else {
            return null;
        }
    }

    /**
     * 将 JsonSchema 转换为 connector schema。
     *
     * @param jsonSchema 输入的 JsonSchema 对象，不可为 null。
     * @param props 额外的属性映射，可用于配置转换过程。
     * @return 转换后的 Connect Schema 对象。如果输入的 JsonSchema 为 null，则返回 null。
     */
    public Schema toConnectSchema(JsonSchema jsonSchema, Map<String, Object> props) {
        if (jsonSchema == null) {
            return null;
        }
        // 尝试从缓存中获取 Schema，如果存在直接返回
        Schema cachedSchema = toConnectSchemaCache.get(jsonSchema);
        if (cachedSchema != null) {
            return cachedSchema;
        }
        // 创建转换上下文
        ToConnectContext ctx = new ToConnectContext();
        // 执行实际的转换逻辑，并将结果存入缓存中
        Schema schema = toConnectSchema(ctx, jsonSchema.rawSchema(), jsonSchema.version(), false, props);
        toConnectSchemaCache.put(jsonSchema, schema);
        return schema;
    }


    private Schema toConnectSchema(ToConnectContext ctx, org.everit.json.schema.Schema jsonSchema) {
        return toConnectSchema(ctx, jsonSchema, null);
    }

    private Schema toConnectSchema(
            ToConnectContext ctx, org.everit.json.schema.Schema jsonSchema, Integer version) {
        return toConnectSchema(ctx, jsonSchema, version, false, Collections.emptyMap()).build();
    }

    private SchemaBuilder toConnectSchema(ToConnectContext ctx,
                                          org.everit.json.schema.Schema jsonSchema,
                                          Integer version,
                                          boolean forceOptional,
                                          Map<String, Object> props) {
        if (jsonSchema == null) {
            return null;
        }
        props = Optional.ofNullable(jsonSchema.getUnprocessedProperties())
                .filter(t -> !t.isEmpty())
                .orElse(props);
        SchemaBuilder builder;
        if (jsonSchema instanceof BooleanSchema) {
            builder = SchemaBuilder.bool();
        } else if (jsonSchema instanceof NumberSchema numberSchema) {
            String type = (String) props.get(CONNECT_TYPE_PROP);
            if (type == null) {
                builder = numberSchema.requiresInteger() ? SchemaBuilder.int64() : SchemaBuilder.float64();
            } else {
                builder = switch (type) {
                    case CONNECT_TYPE_INT8 -> SchemaBuilder.int8();
                    case CONNECT_TYPE_INT16 -> SchemaBuilder.int16();
                    case CONNECT_TYPE_INT32 -> SchemaBuilder.int32();
                    case CONNECT_TYPE_INT64 -> SchemaBuilder.int64();
                    case CONNECT_TYPE_FLOAT32 -> SchemaBuilder.float32();
                    case CONNECT_TYPE_FLOAT64 -> SchemaBuilder.float64();
                    case CONNECT_TYPE_BYTES ->
                            Decimal.builder(MapUtil.get(props, ConnectConstants.DECIMAL_SCALE, Integer.class, 10));
                    default -> throw new IllegalArgumentException("Unsupported type " + type);
                };
            }
        } else if (jsonSchema instanceof StringSchema ss) {
            String type = (String) props.get(CONNECT_TYPE_PROP);
            boolean isBytes = CONNECT_TYPE_BYTES.equals(type);
            if (isBytes) {
                builder = SchemaBuilder.bytes();
            } else {
                String format = ss.getFormatValidator().formatName();
                if ("date-time".equals(format)) {
                    builder = Timestamp.builder()
                            .parameter("format", format);
                } else if ("date".equals(format)) {
                    builder = Date.builder()
                            .parameter("format", format);
                } else if ("time".equals(format)) {
                    builder = Time.builder()
                            .parameter("format", format);
                } else {
                    builder = SchemaBuilder.string();
                }
            }
        } else if (jsonSchema instanceof EnumSchema enumSchema) {
            builder = SchemaBuilder.string();
            String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : JSON_TYPE_ENUM;
            builder.parameter(paramName, "");  // JSON enums have no name, use empty string as placeholder
            int symbolIndex = 0;
            for (Object enumObj : enumSchema.getPossibleValuesAsList()) {
                String enumSymbol = enumObj.toString();
                if (generalizedSumTypeSupport) {
                    builder.parameter(paramName + "." + enumSymbol, String.valueOf(symbolIndex));
                } else {
                    builder.parameter(paramName + "." + enumSymbol, enumSymbol);
                }
                symbolIndex++;
            }
        } else if (jsonSchema instanceof CombinedSchema combinedSchema) {
            CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
            String name;
            if (criterion == CombinedSchema.ONE_CRITERION || criterion == CombinedSchema.ANY_CRITERION) {
                if (generalizedSumTypeSupport) {
                    name = GENERALIZED_TYPE_UNION_PREFIX + ctx.getAndIncrementUnionIndex();
                } else {
                    name = JSON_TYPE_ONE_OF;
                }
            } else if (criterion == CombinedSchema.ALL_CRITERION) {
                return allOfToConnectSchema(ctx, combinedSchema, version, forceOptional);
            } else {
                return SchemaBuilder.string().optional();
            }
            if (combinedSchema.getSubschemas().size() == 2) {
                boolean foundNullSchema = false;
                org.everit.json.schema.Schema nonNullSchema = null;
                for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                    if (subSchema instanceof NullSchema) {
                        foundNullSchema = true;
                    } else {
                        nonNullSchema = subSchema;
                    }
                }
                if (foundNullSchema) {
                    return toConnectSchema(ctx, nonNullSchema, version, true, props);
                }
            }
            int index = 0;
            builder = SchemaBuilder.struct().name(name);
            if (generalizedSumTypeSupport) {
                builder.parameter(GENERALIZED_TYPE_UNION, name);
            }
            for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                if (subSchema instanceof NullSchema) {
                    builder.optional();
                } else {
                    String subFieldName = generalizedSumTypeSupport
                            ? GENERALIZED_TYPE_UNION_FIELD_PREFIX + index
                            : name + ".field." + index;
                    builder.field(subFieldName, toConnectSchema(ctx, subSchema, null, true,
                            subSchema.getUnprocessedProperties()));
                    index++;
                }
            }
        } else if (jsonSchema instanceof ArraySchema arraySchema) {
            org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();
            if (itemsSchema == null) {
                itemsSchema = EmptySchema.builder()
                        .nullable(true)
                        .defaultValue(Collections.emptyList())
                        .build();
            }
            String type = (String) arraySchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type) && itemsSchema instanceof ObjectSchema objectSchema) {
                builder = SchemaBuilder.map(toConnectSchema(ctx, objectSchema.getPropertySchemas().get(KEY_FIELD)),
                        toConnectSchema(ctx, objectSchema.getPropertySchemas().get(VALUE_FIELD)));
            } else {
                builder = SchemaBuilder.array(toConnectSchema(ctx, itemsSchema));
            }
        } else if (jsonSchema instanceof ObjectSchema objectSchema) {
            String type = (String) props.get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type)) {
                builder = SchemaBuilder.map(Schema.STRING_SCHEMA,
                        toConnectSchema(ctx, objectSchema.getSchemaOfAdditionalProperties())
                );
            } else {
                builder = SchemaBuilder.struct();
                ctx.put(objectSchema, builder);
                Map<String, org.everit.json.schema.Schema> properties = objectSchema.getPropertySchemas();
                SortedMap<Integer, Entry<String, org.everit.json.schema.Schema>> sortedMap = new TreeMap<>();
                for (Entry<String, org.everit.json.schema.Schema> property : properties.entrySet()) {
                    org.everit.json.schema.Schema subSchema = property.getValue();
                    Integer index = Optional.ofNullable((Integer) subSchema.getUnprocessedProperties().get(CONNECT_INDEX_PROP))
                            .orElseGet(sortedMap::size);
                    sortedMap.put(index, property);
                }
                for (Entry<String, org.everit.json.schema.Schema> property : sortedMap.values()) {
                    String subFieldName = property.getKey();
                    org.everit.json.schema.Schema subSchema = property.getValue();
                    boolean isFieldOptional = config.useOptionalForNonRequiredProperties()
                            && !objectSchema.getRequiredProperties().contains(subFieldName);

                    SchemaBuilder b = toConnectSchema(ctx, subSchema, null, isFieldOptional,
                            subSchema.getUnprocessedProperties());
                    builder.field(subFieldName, b.build());
                }
            }
        } else if (jsonSchema instanceof ReferenceSchema refSchema) {
            SchemaBuilder refBuilder = ctx.get(refSchema.getReferredSchema());
            if (refBuilder != null) {
                refBuilder.parameter(JSON_ID_PROP, DEFAULT_ID_PREFIX + ctx.incrementAndGetIdIndex());
                return new SchemaWrapper(refBuilder, forceOptional);
            } else {
                return toConnectSchema(ctx, refSchema.getReferredSchema(), version, forceOptional,
                        jsonSchema.getUnprocessedProperties());
            }
        } else if (jsonSchema instanceof EmptySchema es || jsonSchema instanceof NullSchema ns) {
            builder = SchemaBuilder.string().optional();
        } else {
            throw new DataException("Unsupported schema type " + jsonSchema.getClass().getName());
        }

        // Included Kafka Connect version takes priority, fall back to schema registry version
        Integer connectVersion = (Integer) props.get(CONNECT_VERSION_PROP);
        if (connectVersion != null) {
            builder.version(connectVersion);
        } else if (version != null) {
            builder.version(version);
        }
        String description = jsonSchema.getDescription();
        if (description != null) {
            builder.doc(description);
        }
        Map<String, String> parameters = (Map<String, String>) props.get(CONNECT_PARAMETERS_PROP);
        if (parameters != null) {
            builder.parameters(parameters);
        }

        if (jsonSchema.hasDefaultValue()) {
            Object defaultVal = jsonSchema.getDefaultValue();
            JsonNode jsonNode = defaultVal == JSONObject.NULL
                    ? NullNode.getInstance()
                    : OBJECT_MAPPER.convertValue(defaultVal, JsonNode.class);
            builder.defaultValue(toConnectData(builder, jsonNode));
        }

        if (forceOptional) {
            builder.optional();
        }
        String title = jsonSchema.getTitle();
        if (title != null && builder.name() == null) {
            builder.name(title);
        }
        return builder;
    }

    private SchemaBuilder allOfToConnectSchema(ToConnectContext ctx, CombinedSchema combinedSchema,
                                               Integer version, boolean forceOptional) {
        ConstSchema constSchema = null;
        EnumSchema enumSchema = null;
        NumberSchema numberSchema = null;
        StringSchema stringSchema = null;
        CombinedSchema combinedSubschema = null;
        Map<String, org.everit.json.schema.Schema> properties = new LinkedHashMap<>();
        Map<String, Boolean> required = new HashMap<>();
        Map<String, Object> unprocessedProperties = combinedSchema.getUnprocessedProperties();
        for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {

            unprocessedProperties = Optional.ofNullable(subSchema.getUnprocessedProperties())
                    .filter(t -> !t.isEmpty())
                    .orElse(unprocessedProperties);
            if (subSchema instanceof ConstSchema cs) {
                constSchema = cs;
            } else if (subSchema instanceof EnumSchema es) {
                enumSchema = es;
            } else if (subSchema instanceof NumberSchema ns) {
                numberSchema = ns;
            } else if (subSchema instanceof StringSchema ss) {
                stringSchema = ss;
            } else if (subSchema instanceof CombinedSchema cs) {
                combinedSubschema = cs;
            }
            collectPropertySchemas(subSchema, properties, required, new HashSet<>());
        }


        if (!properties.isEmpty()) {
            SchemaBuilder builder = SchemaBuilder.struct();
            ctx.put(combinedSchema, builder);
            for (Entry<String, org.everit.json.schema.Schema> property : properties.entrySet()) {
                String subFieldName = property.getKey();
                org.everit.json.schema.Schema subSchema = property.getValue();
                boolean isFieldOptional = config.useOptionalForNonRequiredProperties()
                        && !required.get(subFieldName);
                SchemaBuilder b = toConnectSchema(ctx, subSchema, null, isFieldOptional, unprocessedProperties);
                builder.field(subFieldName, b.build());
            }
            if (forceOptional) {
                builder.optional();
            }
            return builder;
        } else if (combinedSubschema != null) {
            // Any combined subschema takes precedence over primitive subschemas
            SchemaBuilder builder = toConnectSchema(ctx, combinedSubschema, version, forceOptional, unprocessedProperties);
            trySteDefaultVal(builder, combinedSchema.getDefaultValue());
            return builder;
        } else if (constSchema != null) {
            if (stringSchema != null) {
                // Ignore the const, return the string
                SchemaBuilder builder = toConnectSchema(ctx, stringSchema, version, forceOptional, unprocessedProperties);
                trySteDefaultVal(builder, combinedSchema.getDefaultValue());
                return builder;
            } else if (numberSchema != null) {
                // Ignore the const, return the number or integer
                SchemaBuilder builder = toConnectSchema(ctx, numberSchema, version, forceOptional, unprocessedProperties);
                trySteDefaultVal(builder, combinedSchema.getDefaultValue());
                return builder;
            }
        } else if (enumSchema != null) {
            if (stringSchema != null) {
                // Return a string enum
                SchemaBuilder builder = toConnectSchema(ctx, enumSchema, version, forceOptional, unprocessedProperties);
                trySteDefaultVal(builder, combinedSchema.getDefaultValue());
                return builder;
            } else if (numberSchema != null) {
                // Ignore the enum, return the number or integer
                SchemaBuilder builder = toConnectSchema(ctx, numberSchema, version, forceOptional, unprocessedProperties);
                trySteDefaultVal(builder, combinedSchema.getDefaultValue());
                return builder;
            }
        } else if (stringSchema != null && stringSchema.getFormatValidator() != null && (numberSchema != null)) {
            // This is a number or integer with a format
            SchemaBuilder builder = toConnectSchema(ctx, numberSchema, version, forceOptional, unprocessedProperties);
            trySteDefaultVal(builder, combinedSchema.getDefaultValue());
            return builder;

        }
        throw new IllegalArgumentException("Unsupported criterion "
                + combinedSchema.getCriterion() + " for " + combinedSchema);
    }

    private void trySteDefaultVal(SchemaBuilder builder, Object val) {
        Optional.ofNullable(val)
                .ifPresent(builder::defaultValue);
    }

    private void collectPropertySchemas(
            org.everit.json.schema.Schema schema,
            Map<String, org.everit.json.schema.Schema> properties,
            Map<String, Boolean> required,
            Set<JsonSchema> visited) {
        JsonSchema jsonSchema = new JsonSchema(schema);
        if (visited.contains(jsonSchema)) {
            return;
        } else {
            visited.add(jsonSchema);
        }
        if (schema instanceof CombinedSchema combinedSchema) {
            if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
                for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                    collectPropertySchemas(subSchema, properties, required, visited);
                }
            }
        } else if (schema instanceof ObjectSchema objectSchema) {
            for (Entry<String, org.everit.json.schema.Schema> entry
                    : objectSchema.getPropertySchemas().entrySet()) {
                String fieldName = entry.getKey();
                properties.put(fieldName, entry.getValue());
                required.put(fieldName, objectSchema.getRequiredProperties().contains(fieldName));
            }
        } else if (schema instanceof ReferenceSchema refSchema) {
            collectPropertySchemas(refSchema.getReferredSchema(), properties, required, visited);
        }
    }

    private static boolean isUnionSchema(Schema schema) {
        return JSON_TYPE_ONE_OF.equals(schema.name()) || ConnectUnion.isUnion(schema);
    }

    private interface JsonToConnectTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

    private interface ConnectToJsonLogicalTypeConverter {
        JsonNode convert(Schema schema, Object value, JsonDataConfig config);
    }

    private interface JsonToConnectLogicalTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

    /**
     * Wraps a SchemaBuilder.
     * The internal builder should never be returned, so that the schema is not built prematurely.
     */
    static class SchemaWrapper extends SchemaBuilder {

        private final SchemaBuilder builder;
        // Optional that overrides the one in builder
        private boolean optional;
        // Parameters that override the ones in builder
        private final Map<String, String> parameters;

        public SchemaWrapper(SchemaBuilder builder, boolean optional) {
            super(Type.STRUCT);
            this.builder = builder;
            this.optional = optional;
            this.parameters = new LinkedHashMap<>();
        }

        @Override
        public boolean isOptional() {
            return optional;
        }

        @Override
        public SchemaBuilder optional() {
            optional = true;
            return this;
        }

        @Override
        public SchemaBuilder required() {
            optional = false;
            return this;
        }

        @Override
        public Object defaultValue() {
            return builder.defaultValue();
        }

        @Override
        public SchemaBuilder defaultValue(Object value) {
            builder.defaultValue(value);
            return this;
        }

        @Override
        public String name() {
            return builder.name();
        }

        @Override
        public SchemaBuilder name(String name) {
            builder.name(name);
            return this;
        }

        @Override
        public Integer version() {
            return builder.version();
        }

        @Override
        public SchemaBuilder version(Integer version) {
            builder.version(version);
            return this;
        }

        @Override
        public String doc() {
            return builder.doc();
        }

        @Override
        public SchemaBuilder doc(String doc) {
            builder.doc(doc);
            return this;
        }

        @Override
        public Map<String, String> parameters() {
            Map<String, String> allParameters = new HashMap<>();
            if (builder.parameters() != null) {
                allParameters.putAll(builder.parameters());
            }
            allParameters.putAll(parameters);
            return allParameters;
        }

        @Override
        public SchemaBuilder parameters(Map<String, String> props) {
            parameters.putAll(props);
            return this;
        }

        @Override
        public SchemaBuilder parameter(String propertyName, String propertyValue) {
            parameters.put(propertyName, propertyValue);
            return this;
        }

        @Override
        public Type type() {
            return builder.type();
        }

        @Override
        public List<Field> fields() {
            return builder.fields();
        }

        @Override
        public Field field(String fieldName) {
            return builder.field(fieldName);
        }

        @Override
        public SchemaBuilder field(String fieldName, Schema fieldSchema) {
            builder.field(fieldName, fieldSchema);
            return this;
        }

        @Override
        public Schema keySchema() {
            return builder.keySchema();
        }

        @Override
        public Schema valueSchema() {
            return builder.valueSchema();
        }

        @Override
        public Schema build() {
            // Don't create a ConnectSchema
            return this;
        }

        @Override
        public Schema schema() {
            // Don't create a ConnectSchema
            return this;
        }
    }

    /**
     * Class that holds the context for performing {@code toConnectSchema}
     */
    private static class ToConnectContext {
        private final Map<org.everit.json.schema.Schema, SchemaBuilder> schemaToStructMap;
        private int idIndex = 0;
        private int unionIndex = 0;

        public ToConnectContext() {
            this.schemaToStructMap = new IdentityHashMap<>();
        }

        public SchemaBuilder get(org.everit.json.schema.Schema schema) {
            return schemaToStructMap.get(schema);
        }

        public void put(org.everit.json.schema.Schema schema, SchemaBuilder builder) {
            schemaToStructMap.put(schema, builder);
        }

        public int incrementAndGetIdIndex() {
            return ++idIndex;
        }

        public int getAndIncrementUnionIndex() {
            return unionIndex++;
        }
    }

    /**
     * Class that holds the context for performing {@code fromConnectSchema}
     */
    private static class FromConnectContext {
        private final Set<String> ids;

        public FromConnectContext() {
            this.ids = new HashSet<>();
        }

        public boolean contains(String id) {
            return id != null && ids.contains(id);
        }

        public void add(String id) {
            if (id != null) {
                ids.add(id);
            }
        }
    }
}
