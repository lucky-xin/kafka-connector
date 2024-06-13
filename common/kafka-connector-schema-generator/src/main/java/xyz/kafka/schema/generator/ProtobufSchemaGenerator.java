//package com.pistonint.schema.generator;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.node.NullNode;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.google.protobuf.Descriptors;
//import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
//import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
//import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;
//
//import java.util.Iterator;
//
///**
// * JsonSchema生成器
// *
// * @author chaoxin.lu
// * @version V 1.0
// * @since 2019-04-29 16:27
// */
//public class ProtobufSchemaGenerator {
//
//    enum JacksonTypeToProtobufType {
//
//    }
//
//
//    public static String toSchema(String name, JsonNode jsonSource) {
//        return convertSchema(name, JsonSchemaGenerator.toSchema(jsonSource));
//    }
//
//    public static String convertSchema(String name, ObjectNode jsonschema) {
//        try {
//            DynamicSchema.Builder schema = DynamicSchema.newBuilder();
//            schema.setSyntax(ProtobufSchema.PROTO3);
//            MessageDefinition.Builder message = MessageDefinition.newBuilder(name);
//            String label = null;
//            if (!"object".equals(jsonschema.get("type").asText())) {
//                throw new UnsupportedOperationException("Unsupported array type");
//            }
//            JsonNode properties = jsonschema.get("properties");
//            Iterator<String> fieldNames = properties.fieldNames();
//            while (fieldNames.hasNext()) {
//                String field = fieldNames.next();
//                JsonNode node = jsonschema.get(field);
//                String type = node.get("type").asText();
//                if (node instanceof NullNode) {
//                    MessageDefinition.OneofBuilder oneofBuilder = message.addOneof("_" + field);
//                    oneofBuilder.addField(
//                            true,
//
//                    )
//                }
//
//            }
//
//            schema.addMessageDefinition(message.build());
//            return schema.build().toString();
//        } catch (Descriptors.DescriptorValidationException e) {
//            throw new IllegalStateException(e);
//        }
//    }
//}
