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
    /**
     * 配置定义，用于描述配置项的结构和属性
     */
    protected final ConfigDef configDef;

    /**
     * 配置验证器列表，用于执行具体的配置验证逻辑
     */
    protected final List<ConfigValidator> validators;

    /**
     * 原始配置项映射，保存了用户提供的原始配置信息
     */
    protected final Map<String, String> originals;

    /**
     * 验证后的配置值映射，用于存储经过验证的配置项
     */
    protected final Map<String, ConfigValue> valuesByKey = new HashMap<>();

    /**
     * 解析后的配置映射，用于存储解析后的配置项，以便验证器使用
     */
    protected final Map<String, Object> parsedConfig = new LinkedHashMap<>();

    /**
     * 构造函数，用于初始化配置验证对象
     *
     * @param configDef        配置定义，用于描述配置项的结构和属性，不能为空
     * @param connectorConfigs 连接器配置项映射，不能为空
     * @param validators       配置验证器数组，用于执行具体的配置验证逻辑，不能为空
     * @throws NullPointerException 如果configDef、connectorConfigs或validators为null，或者validators数组中包含null元素
     */
    public ConfigValidation(ConfigDef configDef, Map<String, String> connectorConfigs, ConfigValidator... validators) {
        this.configDef = Objects.requireNonNull(configDef);
        this.originals = ImmutableMap.copyOf(Objects.requireNonNull(connectorConfigs));
        this.validators = ImmutableList.copyOf(Objects.requireNonNull(validators));
        if (this.validators.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("The config validator references may not be null");
        }
    }

    /**
     * 验证配置的入口方法
     *
     * @return 返回验证后的配置对象
     */
    public final Config validate() {
        this.validateIndividualConfigs();
        this.callValidators();
        this.performValidation();
        return new Config(new ArrayList<>(this.valuesByKey.values()));
    }

    /**
     * 验证每个配置项的正确性，并将验证结果添加到valuesByKey中
     */
    protected void validateIndividualConfigs() {
        this.configDef.validate(this.originals()).stream().filter(Objects::nonNull).forEach(this::addConfigValue);
    }

    /**
     * 将验证后的配置值添加到valuesByKey和parsedConfig中
     *
     * @param value 验证后的配置值
     */
    protected void addConfigValue(ConfigValue value) {
        this.valuesByKey.put(value.name(), value);
        this.parsedConfig.put(value.name(), value.value());
    }

    /**
     * 调用每个验证器对配置进行验证
     */
    protected void callValidators() {
        this.validators.forEach(validator -> validator.ensureValid(this.parsedConfig, this));
    }

    /**
     * 执行额外的验证逻辑，可以在子类中重写此方法
     */
    protected void performValidation() {
    }

    /**
     * 创建并返回一个新的配置值对象
     *
     * @param key 配置项的键
     * @return 返回新的配置值对象
     */
    protected ConfigValue newConfigValue(String key) {
        return new ConfigValue(key, this.originals().get(key), new ArrayList<>(), new ArrayList<>());
    }

    /**
     * 返回原始配置项映射
     *
     * @return 返回原始配置项映射
     */
    protected Map<String, String> originals() {
        return this.originals;
    }

    /**
     * 记录配置项的错误信息
     *
     * @param message 错误信息
     * @param key     配置项的键
     */
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

    /**
     * 检查配置项是否有效
     *
     * @param key 配置项的键
     * @return 如果配置项有效返回true，否则返回false
     */
    @Override
    public boolean isValid(String key) {
        ConfigValue value = this.valuesByKey.get(key);
        return value == null || value.errorMessages().isEmpty();
    }

    /**
     * 配置验证器接口，用于执行配置验证逻辑
     */
    @FunctionalInterface
    public interface ConfigValidator {
        /**
         * 验证配置项
         *
         * @param var1
         * @param var2
         */
        void ensureValid(Map<String, Object> var1, ConfigValidationResult var2);
    }
}

