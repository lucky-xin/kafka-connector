package xyz.kafka.connector.formatter.api;


import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.connector.recommenders.Recommenders;
import xyz.kafka.connector.validator.Validators;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * AvailableFormatters
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class AvailableFormatters implements ConfigDef.Recommender, ConfigDef.Validator {
    private final List<String> allowedValues;
    private final List<String> recommendedValues;
    private final ConfigDef.Validator validator;
    private final ConfigDef.Recommender recommender;

    public AvailableFormatters(Collection<String> values) {
        this(values, values);
    }

    public AvailableFormatters(Collection<String> allowedValues, Collection<String> recommendedValues) {
        this.allowedValues = ImmutableList.copyOf(allowedValues);
        this.recommendedValues = ImmutableList.copyOf(recommendedValues);
        this.recommender = Recommenders.anyOf(this.recommendedValues);
        this.validator = Validators.oneOf(this.allowedValues);
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return this.recommender.validValues(name, parsedConfig);
    }

    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {
        return this.recommender.visible(name, parsedConfig);
    }

    @Override
    public void ensureValid(String name, Object value) {
        this.validator.ensureValid(name, value);
    }

    public List<String> allowedValues() {
        return this.allowedValues;
    }

    public List<String> recommendedValues() {
        return this.recommendedValues;
    }

    @Override
    public String toString() {
        return this.validator.toString();
    }
}
