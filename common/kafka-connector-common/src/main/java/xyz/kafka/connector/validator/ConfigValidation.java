package xyz.kafka.connector.validator;

import cn.hutool.core.text.CharSequenceUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ConfigValidation
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class ConfigValidation implements ConfigValidationResult {
    protected final ConfigDef configDef;
    protected final List<ConfigValidator> validators;
    protected final Map<String, String> originals;
    protected final Map<String, ConfigValue> valuesByKey = new HashMap<>();
    protected final Map<String, Object> parsedConfig = new LinkedHashMap<>();

    public ConfigValidation(ConfigDef configDef, Map<String, String> connectorConfigs, ConfigValidator... validators) {
        this.configDef = Objects.requireNonNull(configDef);
        this.originals = ImmutableMap.copyOf(Objects.requireNonNull(connectorConfigs));
        this.validators = ImmutableList.copyOf(Objects.requireNonNull(validators));
        if (this.validators.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("The config validator references may not be null");
        }
    }

    public final Config validate() {
        this.validateIndividualConfigs();
        this.callValidators();
        this.performValidation();
        return new Config(new ArrayList<>(this.valuesByKey.values()));
    }

    protected void validateIndividualConfigs() {
        this.configDef.validate(this.originals()).stream().filter(Objects::nonNull).forEach(this::addConfigValue);
    }

    protected void addConfigValue(ConfigValue value) {
        this.valuesByKey.put(value.name(), value);
        this.parsedConfig.put(value.name(), value.value());
    }

    protected void callValidators() {
        this.validators.forEach(validator -> validator.ensureValid(this.parsedConfig, this));
    }

    protected void performValidation() {
    }

    protected ConfigValue newConfigValue(String key) {
        return new ConfigValue(key, this.originals().get(key), new ArrayList<>(), new ArrayList<>());
    }

    protected Map<String, String> originals() {
        return this.originals;
    }

    @Override
    public void recordError(String message, String key) {
        Objects.requireNonNull(key);
        if (CharSequenceUtil.isNotEmpty(key)) {
            ConfigValue value = this.valuesByKey.computeIfAbsent(key, this::newConfigValue);
            if (CharSequenceUtil.isNotEmpty(message)) {
                value.addErrorMessage(message);
            }
        }

    }

    @Override
    public boolean isValid(String key) {
        ConfigValue value = this.valuesByKey.get(key);
        return value == null || value.errorMessages().isEmpty();
    }

    @FunctionalInterface
    public interface ConfigValidator {
        void ensureValid(Map<String, Object> var1, ConfigValidationResult var2);
    }
}
