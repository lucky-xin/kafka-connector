package xyz.kafka.serialization.strategy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Map;

/**
 * Schema和主题映射器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-16
 */
public class TopicNameStrategy implements SubjectNameStrategy {

    private String prefix;
    private String suffix;

    @Override
    public boolean usesSchema() {
        return false;
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix).append(".");
        }
        sb.append(topic);
        if (suffix != null) {
            sb.append(".").append(suffix);
        }
        return sb.toString();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.prefix = (String) configs.get("prefix");
        this.suffix = (String) configs.get("suffix");
    }
}
