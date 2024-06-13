package xyz.kafka.connector.recommenders;

import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * VisibleIfRecommender
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class VisibleIfRecommender implements ConfigDef.Recommender {
    final String configKey;
    final Predicate<Object> enabledPredicate;
    final ValidValuesCallback validValuesCallback;

    public VisibleIfRecommender(String configKey, Object value, ValidValuesCallback validValuesCallback) {
        this(configKey, (Predicate<?>) value::equals, validValuesCallback);
    }


    @SuppressWarnings("unchecked")
    public VisibleIfRecommender(
            String configKey,
            Predicate<?> enabledPredicate,
            ValidValuesCallback validValuesCallback) {
        Objects.requireNonNull(configKey, "configKey cannot be null.");
        Objects.requireNonNull(enabledPredicate, "predicate cannot be null.");
        Objects.requireNonNull(validValuesCallback, "validValuesCallback cannot be null.");
        this.configKey = configKey;
        this.enabledPredicate = (Predicate<Object>) enabledPredicate;
        this.validValuesCallback = validValuesCallback;
    }

    @Override
    public List<Object> validValues(String s, Map<String, Object> map) {
        return this.validValuesCallback.validValues(s, map);
    }

    @Override
    public boolean visible(String key, Map<String, Object> settings) {
        return this.enabledPredicate.test(settings.get(this.configKey));
    }
}
