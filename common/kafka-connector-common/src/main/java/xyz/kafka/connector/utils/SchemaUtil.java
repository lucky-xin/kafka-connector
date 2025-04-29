package xyz.kafka.connector.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
            Object defaultVal = null;
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
                    Map<Object, Object> ma = new HashMap<>();
                    Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
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
                    Map<String, Object> map = new HashMap<>();
                    for (Field field : schema.fields()) {
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

    public static org.apache.kafka.connect.data.Schema getConnectSchema(SchemaRegistryClient cli, String subject) {
        try {
            org.apache.kafka.connect.data.Schema schema = null;
            SchemaMetadata mete = cli.getLatestSchemaMetadata(subject);
            if ("JSON".equals(mete.getSchemaType())) {
                JsonData jsonData = new JsonData();
                schema = jsonData.toConnectSchema(new JsonSchema(mete.getSchema()), Map.of());
            } else if ("AVRO".equals(mete.getSchemaType())) {
                AvroData avroData = new AvroData(1);
                org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(mete.getSchema());
                schema = avroData.toConnectSchema(avroSchema);
            } else if ("PROTOBUF".equals(mete.getSchemaType())) {
                ProtobufData protobufData = new ProtobufData();
                schema = protobufData.toConnectSchema(new ProtobufSchema(mete.getSchema()));
            }
            return schema;
        } catch (Exception e) {
            throw new ConnectException("get latest schema metadata error", e);
        }
    }
}
