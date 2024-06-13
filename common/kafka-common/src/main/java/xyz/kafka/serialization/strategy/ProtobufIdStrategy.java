package xyz.kafka.serialization.strategy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Map;

/**
 * 默认主键schema映射器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-16
 */
public class ProtobufIdStrategy implements SubjectNameStrategy {

    @Override
    public boolean usesSchema() {
        return false;
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        return "id.proto";
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
