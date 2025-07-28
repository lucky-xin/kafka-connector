package xyz.kafka.connector.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import xyz.kafka.serialization.json.JsonData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Kafka connect schema工具类
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2022-12-26
 */
public class SchemaUtil {

    public static Object fieldVal(Schema schema, JsonNode jsonNode) {
        if (schema == null || jsonNode == null) {
            return null;
        }
        try {
            Object defaultVal;
            switch (schema.type()) {
                case INT8, INT32, INT16 -> {
                    defaultVal = Optional.ofNullable(schema.defaultValue()).orElse(-1);
                    return jsonNode.asInt((Integer) defaultVal);
                }
                case INT64 -> {
                    defaultVal = Optional.ofNullable(schema.defaultValue()).orElse(-1L);
                    return jsonNode.asLong((Long) defaultVal);
                }
                case BOOLEAN -> {
                    defaultVal = Optional.ofNullable(schema.defaultValue()).orElse(false);
                    return jsonNode.asBoolean((Boolean) defaultVal);
                }
                case FLOAT32 -> {
                    defaultVal = Optional.ofNullable(schema.defaultValue()).orElse(-1F);
                    return jsonNode.asDouble((Float) defaultVal);
                }
                case FLOAT64 -> {
                    defaultVal = Optional.ofNullable(schema.defaultValue()).orElse(-1D);
                    return jsonNode.asDouble((Double) defaultVal);
                }
                case STRING -> {
                    return jsonNode.asText((String) schema.defaultValue());
                }
                case ARRAY -> {
                    ArrayNode arrayNode = (ArrayNode) jsonNode;
                    List<Object> vals = new ArrayList<>(arrayNode.size());
                    for (int i = 0; i < arrayNode.size(); i++) {
                        Object v = fieldVal(schema.valueSchema(), arrayNode.get(i));
                        if (v == null && schema.isOptional()) {
                            continue;
                        }
                        vals.add(v);
                    }
                    return vals;
                }
                case MAP -> {
                    Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
                    Map<Object, Object> ma = new HashMap<>(jsonNode.size());
                    while (it.hasNext()) {
                        Map.Entry<String, JsonNode> entry = it.next();
                        String key = entry.getKey();
                        JsonNode value = entry.getValue();
                        Object v = fieldVal(schema.valueSchema(), value);
                        if (v == null && schema.isOptional()) {
                            continue;
                        }
                        ma.put(key, v);
                    }
                    return ma;
                }
                case STRUCT -> {
                    List<Field> fields = schema.fields();
                    Map<String, Object> map = new HashMap<>(fields.size());
                    for (Field field : fields) {
                        Object v = fieldVal(field.schema(), jsonNode.get(field.name()));
                        if (v == null && field.schema().isOptional()) {
                            continue;
                        }
                        map.put(field.name(), v);
                    }
                    return map;
                }
                case BYTES -> {
                    return jsonNode.binaryValue();
                }
                default -> {
                }
            }
        } catch (Exception t) {
            throw new IllegalStateException(t);
        }
        return null;
    }

    public static Schema getConnectSchema(SchemaRegistryClient cli, String subject) {
        try {
            Schema schema = null;
            SchemaMetadata mete = cli.getLatestSchemaMetadata(subject);
            if (JsonSchema.TYPE.equals(mete.getSchemaType())) {
                JsonData jsonData = new JsonData();
                schema = jsonData.toConnectSchema(new JsonSchema(mete.getSchema()), Map.of());
            } else if (AvroSchema.TYPE.equals(mete.getSchemaType())) {
                AvroData avroData = new AvroData(1);
                org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(mete.getSchema());
                schema = avroData.toConnectSchema(avroSchema);
            } else if (ProtobufSchema.TYPE.equals(mete.getSchemaType())) {
                ProtobufData protobufData = new ProtobufData();
                schema = protobufData.toConnectSchema(new ProtobufSchema(mete.getSchema()));
            }
            return schema;
        } catch (Exception e) {
            throw new ConnectException("get latest schema metadata error", e);
        }
    }

    /**
     * 根据 JsonNode 推断出对应的 Schema
     *
     * @param jsonNode 输入的 JSON 节点
     * @return 推断出的 Kafka Connect Schema
     */
    public static Schema inferSchema(JsonNode jsonNode) {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }
        JsonNodeType nodeType = jsonNode.getNodeType();
        return switch (nodeType) {
            case POJO, OBJECT -> inferObjectSchema(jsonNode);
            case ARRAY -> inferArraySchema(jsonNode);
            case BINARY -> Schema.OPTIONAL_BYTES_SCHEMA;
            case BOOLEAN -> Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case NUMBER -> {
                if (jsonNode.isInt()) {
                    yield Schema.OPTIONAL_INT32_SCHEMA;
                } else if (jsonNode.isLong()) {
                    yield Schema.OPTIONAL_INT64_SCHEMA;
                } else if (jsonNode.isFloat()) {
                    yield Schema.OPTIONAL_FLOAT32_SCHEMA;
                } else if (jsonNode.isDouble()) {
                    yield Schema.OPTIONAL_FLOAT64_SCHEMA;
                } else if (jsonNode.isBigDecimal()) {
                    yield Schema.OPTIONAL_FLOAT64_SCHEMA;
                }
                yield Schema.OPTIONAL_FLOAT64_SCHEMA;
            }
            case STRING -> Schema.OPTIONAL_STRING_SCHEMA;
            case NULL, MISSING -> null;
        };
    }

    /**
     * 推断数组类型 Schema
     * 目前策略：数组内元素类型必须一致，以第一个非 null 元素的类型为准
     */
    private static Schema inferArraySchema(JsonNode arrayNode) {
        if (arrayNode.isEmpty()) {
            // 空数组，返回包含可选任意类型的数组
            return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
        }

        // 取第一个非 null 元素推断类型
        JsonNode firstNonNullElement = null;
        for (JsonNode element : arrayNode) {
            if (!element.isNull()) {
                firstNonNullElement = element;
                break;
            }
        }

        Schema elementSchema;
        if (firstNonNullElement == null) {
            // 全部是 null，则元素类型为 nullable string（或 optional null）
            elementSchema = Schema.OPTIONAL_STRING_SCHEMA;
        } else {
            elementSchema = inferSchema(firstNonNullElement);
        }

        return SchemaBuilder.array(elementSchema).optional().build();
    }

    /**
     * 推断对象类型 Schema → 对应 CONNECT 的 STRUCT
     */
    private static Schema inferObjectSchema(JsonNode objectNode) {
        SchemaBuilder structBuilder = SchemaBuilder.struct().optional();
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode fieldValue = entry.getValue();
            if (fieldValue == null || fieldValue.isNull()) {
                continue;
            }
            Schema fieldSchema = inferSchema(fieldValue);
            structBuilder.field(fieldName, fieldSchema);
        }

        return structBuilder.build();
    }
}
