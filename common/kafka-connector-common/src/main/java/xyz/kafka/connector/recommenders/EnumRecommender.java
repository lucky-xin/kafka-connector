package xyz.kafka.connector.recommenders;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * EnumRecommender
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class EnumRecommender<T extends Enum<?>> implements ConfigDef.Validator, ConfigDef.Recommender {
    private final Set<String> validValues;
    private final VisibleCallback visible;

    public EnumRecommender(Class<T> enumClass) {
        this.visible = VisibleCallback.ALWAYS_VISIBLE;
        Set<String> validEnums = new LinkedHashSet<>();
        for (T o : enumClass.getEnumConstants()) {
            validEnums.add(o.name().toLowerCase());
            validEnums.add(o.name().toUpperCase());
        }
        this.validValues = Set.copyOf(validEnums);
    }

    public EnumRecommender(Set<String> validValues) {
        this.visible = VisibleCallback.ALWAYS_VISIBLE;
        this.validValues = validValues;
    }

    @SafeVarargs
    public EnumRecommender(Class<T> enumClass,
                           Function<String, String> conversion,
                           VisibleCallback visible,
                           T... excludedValues) {
        this.visible = visible;
        Set<String> validEnums = new LinkedHashSet<>();
        for (T o : enumClass.getEnumConstants()) {
            validEnums.add(conversion.apply(o.toString()));
        }
        for (T t : excludedValues) {
            validEnums.remove(conversion.apply(t.toString()));
        }
        this.validValues = Set.copyOf(validEnums);
    }

    @Override
    public void ensureValid(String key, Object value) {
        if (value != null && !this.validValues.contains(value.toString())) {
            throw new ConfigException(key, value, "Invalid enumerator");
        }
    }

    @Override
    public String toString() {
        return this.validValues.toString();
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
        return ImmutableList.copyOf(this.validValues);
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
        return this.visible.visible(name, connectorConfigs);
    }
}
