package xyz.kafka.utils;

import cn.hutool.core.util.StrUtil;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ConfigUtil
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class ConfigUtil {

    public static Map<String, String> getRestHeaders(String envName) {
        return Optional.ofNullable(System.getenv(envName))
                .filter(StrUtil::isNotEmpty)
                .map(t -> t.split(";"))
                .stream()
                .flatMap(Stream::of)
                .filter(StrUtil::isNotEmpty)
                .map(t -> t.split(":"))
                .collect(Collectors.toMap(t -> t[0], t -> t[1], (v1, v2) -> v2));
    }

    @SuppressWarnings("all")
    public static Map<Object, Object> addDefaultEnvValue(Map<?, ?> props) {
        Map<Object, String> params = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "KAFKA_SCHEMA_REGISTRY_SVC_ENDPOINT");
        Map<Object, Object> m = new HashMap<>(props);
        for (Map.Entry<Object, String> entry : params.entrySet()) {
            String tmp = null;
            if (props.containsKey(entry.getKey()) || StrUtil.isEmpty(tmp = System.getenv(entry.getValue()))) {
                continue;
            }
            m.put(entry.getKey(), tmp);
        }
        return m;
    }
}
