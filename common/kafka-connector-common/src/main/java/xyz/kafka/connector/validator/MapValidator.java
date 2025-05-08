package xyz.kafka.connector.validator;

import cn.hutool.core.text.CharSequenceUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * MapValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class MapValidator extends Validators.SingleOrListValidator {
    private final ConfigDef.Validator keyValidator;
    private final ConfigDef.Validator valueValidator;


    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> parseMap(String value, Function<String, K> keyParser, Function<String, V> valueParser) {
        Map<K, V> result = new HashMap<>(8);
        ((List<String>) ConfigDef.parseType("", value, ConfigDef.Type.LIST))
                .stream()
                .filter(CharSequenceUtil::isNotEmpty).forEach(s -> {
                    String[] entry = s.split("=", 2);
                    result.put(keyParser.apply(entry[0].trim()), valueParser.apply(entry[1].trim()));
                });
        return ImmutableMap.copyOf(result);
    }

    public MapValidator(ConfigDef.Validator keyValidator, ConfigDef.Validator valueValidator) {
        this.keyValidator = Objects.requireNonNull(keyValidator);
        this.valueValidator = Objects.requireNonNull(valueValidator);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void validate(String name, Object value) {
        if (!(value instanceof String)) {
            throw new ConfigException(name, "Must be a non-null string.");
        }
        Set<String> keys = new HashSet<>();
        List<String> entries = (List<String>) ConfigDef.parseType(name, value, ConfigDef.Type.LIST);
        for (String entry : entries) {
            if (CharSequenceUtil.isNotEmpty(entry)) {
                try {
                    String[] pair = entry.split("=", 2);
                    String key = pair[0].trim();
                    String val = pair[1].trim();
                    this.keyValidator.ensureValid(name, key);
                    this.valueValidator.ensureValid(name, val);
                    if (!keys.add(key)) {
                        throw new ConfigException(name, value, String.format("key '%s' is specified multiple times", key));
                    }
                } catch (ConfigException e) {
                    throw e;
                } catch (Exception e2) {
                    throw new ConfigException(name, value, "Must contain valid key=value pairs separated by ',': " + e2.getMessage());
                }
            }
        }
        entries.stream().filter(CharSequenceUtil::isNotEmpty).forEach(s -> {
        });
    }

    @Override
    public String toString() {
        return String.format("Comma-separated key-value pairs, where keys %s and values %s", this.keyValidator, this.valueValidator);
    }
}
