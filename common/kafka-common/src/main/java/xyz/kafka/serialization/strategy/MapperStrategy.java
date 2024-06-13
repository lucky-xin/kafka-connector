package xyz.kafka.serialization.strategy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 给定Subject名称策略
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-16
 */
public class MapperStrategy implements SubjectNameStrategy {

    private Map<String, String> subjectNameMapper = Collections.emptyMap();

    @Override
    public boolean usesSchema() {
        return false;
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        return Optional.ofNullable(subjectNameMapper.get(topic))
                .orElseGet(() -> subjectNameMapper.get("*"));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.subjectNameMapper = Stream.ofNullable((String) configs.get("mapper"))
                .map(t -> t.split(","))
                .flatMap(Stream::of)
                .map(t -> t.split(":"))
                .collect(Collectors.toMap(t -> t[0].strip(), t -> t[1].strip(), (v1, v2) -> v2));

    }
}
