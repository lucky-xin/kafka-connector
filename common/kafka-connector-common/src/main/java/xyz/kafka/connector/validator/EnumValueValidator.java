package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * EnumValueValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class EnumValueValidator<T extends Enum<?>> implements ConfigDef.Validator, ConfigDef.Recommender {

    private final List<Object> validValues;
    private final Class<T> enumClass;

    public EnumValueValidator(Class<T> enumClass, Function<T, Object> val) {
        this.enumClass = enumClass;
        this.validValues = Stream.of(enumClass.getEnumConstants())
                .map(t -> val.apply(t).toString().toLowerCase())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public void ensureValid(String key, Object value) {
        if (value == null) {
            return;
        }
        String enumValue = value.toString().toLowerCase();
        if (!validValues.contains(enumValue)) {
            throw new ConfigException(key, value, "Value must be one of: " + this);
        }
    }

    @Override
    public String toString() {
        return validValues.toString();
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
        return validValues;
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
        return true;
    }
}
