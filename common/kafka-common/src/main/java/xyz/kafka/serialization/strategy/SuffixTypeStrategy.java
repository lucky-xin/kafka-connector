package xyz.kafka.serialization.strategy;

import cn.hutool.core.map.MapUtil;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Map;

/**
 * SuffixTypeStrategy
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-16
 */
public class SuffixTypeStrategy implements SubjectNameStrategy {

    @Override
    public boolean usesSchema() {
        return false;
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        StringBuilder sb = new StringBuilder(topic);
        if (schema instanceof AvroSchema) {
            sb.append(".avro").append(".");
        } else if (schema instanceof JsonSchema) {
            sb.append(".json").append(".");
        } else if (schema instanceof ProtobufSchema) {
            sb.append(".proto").append(".");
        }
        return sb.toString();
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
