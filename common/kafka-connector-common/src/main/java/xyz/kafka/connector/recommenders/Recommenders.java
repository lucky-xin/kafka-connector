package xyz.kafka.connector.recommenders;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * Recommenders
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class Recommenders {
    private Recommenders() {
    }

    public static ConfigDef.Recommender visibleIf(String configKey, Predicate<Object> enabledPredicate) {
        return new VisibleIfRecommender(configKey, enabledPredicate, ValidValuesCallback.EMPTY);
    }

    public static ConfigDef.Recommender visibleIf(String configKey, Set<Object> allowedValues) {
        return new VisibleIfRecommender(configKey, allowedValues::contains, ValidValuesCallback.EMPTY);
    }

    public static ConfigDef.Recommender visibleIf(String configKey, Object value) {
        return visibleIf(configKey, value, ValidValuesCallback.EMPTY);
    }

    public static ConfigDef.Recommender visibleIf(String configKey, Object value,
                                                  ValidValuesCallback validValuesCallback) {
        return new VisibleIfRecommender(configKey, value, validValuesCallback);
    }

    public static ConfigDef.Recommender visibleIf(String configKey, Object value,
                                                  ConfigDef.Recommender validValuesRecommender) {
        ValidValuesCallback validValuesCallback;
        if (validValuesRecommender != null) {
            validValuesCallback = validValuesRecommender::validValues;
        } else {
            validValuesCallback = ValidValuesCallback.EMPTY;
        }
        return visibleIf(configKey, value, validValuesCallback);
    }

    public static ConfigDef.Recommender visibleIf(String configKey,
                                                  Predicate<Object> enabledPredicate,
                                                  ConfigDef.Recommender validValuesRecommender) {
        ValidValuesCallback validValuesCallback;
        if (validValuesRecommender != null) {
            validValuesCallback = validValuesRecommender::validValues;
        } else {
            validValuesCallback = ValidValuesCallback.EMPTY;
        }
        return new VisibleIfRecommender(configKey, enabledPredicate, validValuesCallback);
    }

    @SafeVarargs
    public static <T extends Enum<?>> ConfigDef.Recommender enumValues(Class<T> enumClass, T... excludes) {
        return enumValues(enumClass, VisibleCallback.ALWAYS_VISIBLE, excludes);
    }

    @SafeVarargs
    public static <T extends Enum<?>> ConfigDef.Recommender enumValues(Class<T> enumClass, VisibleCallback visible, T... excludes) {
        return enumValues(enumClass, visible, String::toLowerCase, excludes);
    }

    @SafeVarargs
    public static <T extends Enum<?>> ConfigDef.Recommender enumValues(Class<T> enumClass,
                                                                       VisibleCallback visible,
                                                                       Function<String, String> conversion,
                                                                       T... excludes) {
        Function<String, String> function = String::toLowerCase;
        return new EnumRecommender<>(enumClass, conversion != null ?
                conversion : function, visible != null ? visible : VisibleCallback.ALWAYS_VISIBLE, excludes);
    }

    public static <T, V> ConfigDef.Recommender anyOf(Iterable<T> allowedValues, Function<T, V> mapping) {
        return anyOf(StreamSupport.stream(allowedValues.spliterator(), false).map(mapping).toList());
    }

    public static ConfigDef.Recommender anyOf(Iterable<?> allowedValues) {
        List<Object> values = ImmutableList.copyOf(allowedValues);
        return new ConfigDef.Recommender() {
            @Override
            public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                return values;
            }

            @Override
            public boolean visible(String name, Map<String, Object> parsedConfig) {
                return true;
            }
        };
    }

    public static ConfigDef.Recommender anyOf(Object... allowedValues) {
        return anyOf(ImmutableList.copyOf(allowedValues));
    }

    public static ConfigDef.Recommender charset() {
        return charset(VisibleCallback.ALWAYS_VISIBLE, Charset.availableCharsets().keySet());
    }

    public static ConfigDef.Recommender charset(VisibleCallback visible) {
        return charset(visible, Charset.availableCharsets().keySet());
    }

    public static ConfigDef.Recommender charset(VisibleCallback visible, String... charsets) {
        return charset(visible, ImmutableList.copyOf(charsets));
    }

    public static ConfigDef.Recommender charset(VisibleCallback visible, Iterable<String> charsets) {
        return new CharsetRecommender(charsets, visible);
    }
}
