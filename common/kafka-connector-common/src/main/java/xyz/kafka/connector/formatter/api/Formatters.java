package xyz.kafka.connector.formatter.api;


import cn.hutool.core.text.CharSequenceUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.recommenders.Recommenders;
import xyz.kafka.connector.utils.ConfigKeys;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * FormatterProviderNotFoundException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
@SuppressWarnings("all")
public class Formatters {
    private static final Logger LOG = LoggerFactory.getLogger(Formatters.class);
    private static final Map<String, FormatterProvider> REGISTRY = new ConcurrentSkipListMap<>();
    private static final Map<String, String> ALIASES = new ConcurrentSkipListMap<>();
    protected static final String DEFAULT_GROUP_NAME = "Formatter";

    private static void loadAllFormatters() {
        LOG.debug("Searching for and loading all record FormatterProvider implementations on the classpath");
        AtomicInteger count = new AtomicInteger();

        ServiceLoader<FormatterProvider> loadedFormatters = ServiceLoader.load(FormatterProvider.class, FormatterProvider.class.getClassLoader());
        Iterator<FormatterProvider> formatterIterator = loadedFormatters.iterator();

        try {
            while (formatterIterator.hasNext()) {
                try {
                    FormatterProvider provider = formatterIterator.next();
                    Class.forName(provider.formatterClass().getName());
                    REGISTRY.put(provider.name(), provider);
                    count.incrementAndGet();
                    LOG.debug("Found FormatterProvider '{}' {}", provider.name(), provider.formatterClass().getName());
                } catch (Throwable var4) {
                    LOG.warn("Skipping FormatterProvider provider after error while loading. This often means the formatter implementation was missing some of its dependencies, or the FormatterProvider implementation was improperly defined.", var4);
                }
            }
        } catch (Throwable var5) {
            LOG.error("Error loading formatter providers. This will likely hinder the functionality of the connector.", var5);
        }
        LOG.debug("Registered {} formatter providers", count.get());
    }

    public static void registerAlias(Class<? extends FormatterProvider> providerClass, String... aliases) {
        FormatterProvider provider = findRegistered(providerClass);
        if (provider == null) {
            try {
                provider = providerClass.getConstructor().newInstance();
            } catch (Exception var4) {
                throw new ConnectException(String.format("Unable to instantiate FormatterProvider %s", providerClass.getName()), var4);
            }
        }

        registerAlias(provider, aliases);
    }

    public static void registerAlias(String providerName, String... aliases) {
        FormatterProvider provider = REGISTRY.get(providerName);
        if (provider == null) {
            throw new FormatterProviderNotFoundException(String.format("The '%s' FormatProvider was not found", providerName));
        } else {
            registerAlias(provider, aliases);
        }
    }

    public static void registerAlias(FormatterProvider provider, String... aliases) {
        FormatterProvider prov = REGISTRY.computeIfAbsent(provider.name(), name -> {
            LOG.debug("Registering FormatterProvider '{}' {}", provider.name(), provider.formatterClass().getName());
            return provider;
        });
        for (String alias : aliases) {
            ALIASES.computeIfAbsent(alias, a -> {
                LOG.debug("Registering FormatterProvider alias '{}' to {}", alias, prov.name());
                return prov.name();
            });
        }
    }

    public static AvailableFormatters availableFormatters() {
        return availableFormatters(name -> true, name -> true);
    }

    public static AvailableFormatters availableFormatters(Predicate<String> validFilter, Predicate<String> recommendedFilter) {
        Set<String> allowedValues = new LinkedHashSet<>();
        REGISTRY.keySet().stream().filter(validFilter).forEach(allowedValues::add);
        ALIASES.keySet().stream().filter(validFilter).forEach(allowedValues::add);
        Set<String> recommendedValues = new LinkedHashSet<>();
        REGISTRY.keySet().stream().filter(validFilter).forEach(recommendedValues::add);
        ALIASES.entrySet().stream()
                .filter(e -> recommendedFilter.test(e.getKey()))
                .forEach(e -> {
                    recommendedValues.remove(e.getValue());
                    recommendedValues.add(e.getKey());
                });
        return new AvailableFormatters(allowedValues, recommendedValues);
    }

    public static Formatter createFormatter(AbstractConfig connectorConfig, String formatterConfigKey) throws FormatterProviderNotFoundException, ConfigException {
        String formatterName = connectorConfig.getString(formatterConfigKey);
        FormatterProvider provider = provider(formatterName);
        if (provider != null) {
            String prefix = formatterConfigKey + ".";
            Map<String, ?> formatterConfigs = connectorConfig.originalsWithPrefix(prefix, true);
            return provider.create(formatterConfigs);
        } else {
            Set<String> aliasesAndNames = new HashSet<>(ALIASES.keySet());
            aliasesAndNames.addAll(REGISTRY.keySet());
            throw new FormatterProviderNotFoundException(String.format("Unable to find the formatter '%s' in the available set: %s", formatterName, String.join(",", aliasesAndNames)));
        }
    }

    public static Collection<FormatterProvider> registeredFormatterProviders() {
        return new HashSet<>(REGISTRY.values());
    }

    public static FormatterProvider provider(String providerNameOrAlias) {
        String className = ALIASES.getOrDefault(providerNameOrAlias, providerNameOrAlias);
        return REGISTRY.get(className);
    }

    public static ConfigKeys formatterConfigKeys(String formatterConfigName) {
        return formatterConfigKeys(formatterConfigName, true, (AvailableFormatters) null);
    }

    public static ConfigKeys formatterConfigKeys(String formatterConfigName, boolean includeFormatterConfigKeys) {
        return formatterConfigKeys(formatterConfigName, includeFormatterConfigKeys, (AvailableFormatters) null);
    }

    public static ConfigKeys formatterConfigKeys(String formatterConfigName, AvailableFormatters availableFormatters) {
        return formatterConfigKeys(formatterConfigName, true, availableFormatters);
    }

    public static ConfigKeys formatterConfigKeys(String formatterConfigName, boolean includeFormatterConfigKeys, AvailableFormatters availableFormatters) {
        Objects.requireNonNull(formatterConfigName);
        if (CharSequenceUtil.isEmpty(formatterConfigName)) {
            throw new IllegalArgumentException("The name of the configuration key may not only be whitespace");
        } else {
            if (availableFormatters == null) {
                availableFormatters = availableFormatters();
            }

            ConfigKeys keys = new ConfigKeys();
            ConfigKeys.Key formatterKey = keys.define(formatterConfigName, ConfigDef.Type.STRING)
                    .group(DEFAULT_GROUP_NAME)
                    .noDefaultValue()
                    .importance(ConfigDef.Importance.HIGH)
                    .width(ConfigDef.Width.MEDIUM)
                    .validator(availableFormatters)
                    .recommender(availableFormatters);
            if (includeFormatterConfigKeys) {
                String prefix = formatterConfigName + ".";
                availableFormatters.recommendedValues().stream().map(Formatters::provider).distinct().forEach((provider) -> {
                    ConfigKeys providerKeys = provider.configs();
                    providerKeys.alter(k -> applyVisibilityRecommender(formatterConfigName, provider, k));
                    providerKeys.prefixKeyNames(prefix);
                    keys.getOrDefine(providerKeys).forEach(k -> formatterKey.addDependent(k.name()));
                });
                keys.stream().filter(key -> key.name().startsWith(prefix)).forEach(formatterKey::addDependent);
            }

            return keys;
        }
    }

    protected static void applyVisibilityRecommender(String formatterConfigKey, FormatterProvider provider, ConfigKeys.Key key) {
        Set<String> aliases = nameAndAliasesFor(provider);
        ConfigDef.Recommender recommender = Recommenders.visibleIf(formatterConfigKey, aliases::contains, key.recommender());
        key.recommender(recommender);
    }

    static FormatterProvider findRegistered(Class<? extends FormatterProvider> providerClass) {
        return REGISTRY.values().stream()
                .filter(provider -> providerClass.equals(provider.getClass()))
                .findFirst()
                .orElse(null);
    }

    protected static Set<String> nameAndAliasesFor(FormatterProvider provider) {
        Set<String> all = new HashSet<>();
        all.add(provider.name());
        all.addAll(aliasesFor(provider.name()));
        return all;
    }

    private static Set<String> aliasesFor(String nameOrAlias) {
        return ALIASES.entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(nameOrAlias))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    static {
        loadAllFormatters();
    }

    public static class ConfigKeyBuilder {
        private final String formatterConfigKey;
        private Object defaultFormat;
        private String groupName;
        private int orderInGroup;
        private ConfigDef.Importance importance;
        private ConfigDef.Width width;
        private String displayName;
        private String documentation;
        private AvailableFormatters availableFormatters;
        private boolean includeFormatterConfigKeys;

        public static ConfigKeyBuilder withName(String formatterConfigKeyName) {
            return new ConfigKeyBuilder(formatterConfigKeyName);
        }

        ConfigKeyBuilder(String name) {
            this.defaultFormat = ConfigDef.NO_DEFAULT_VALUE;
            this.groupName = DEFAULT_GROUP_NAME;
            this.orderInGroup = 1;
            this.importance = ConfigDef.Importance.HIGH;
            this.width = ConfigDef.Width.LONG;
            this.availableFormatters = Formatters.availableFormatters();
            this.includeFormatterConfigKeys = true;
            this.formatterConfigKey = name;
            this.displayName = name;
        }

        public ConfigKeyBuilder defaultFormat(String defaultFormat) {
            this.defaultFormat = defaultFormat;
            return this;
        }

        public ConfigKeyBuilder group(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public ConfigKeyBuilder orderInGroup(int orderInGroup) {
            this.orderInGroup = orderInGroup;
            return this;
        }

        public ConfigKeyBuilder importance(ConfigDef.Importance importance) {
            this.importance = importance;
            return this;
        }

        public ConfigKeyBuilder width(ConfigDef.Width width) {
            this.width = width;
            return this;
        }

        public ConfigKeyBuilder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public ConfigKeyBuilder documentation(String documentation) {
            this.documentation = documentation;
            return this;
        }

        public ConfigKeyBuilder availableFormatters(AvailableFormatters af) {
            this.availableFormatters = af != null ? af : Formatters.availableFormatters();
            return this;
        }

        public ConfigKeyBuilder includeFormatConfigKeys(boolean include) {
            this.includeFormatterConfigKeys = include;
            return this;
        }

        public void build(ConfigKeys keys) {
            Objects.requireNonNull(keys);
            ConfigKeys formatterKeys = Formatters.formatterConfigKeys(
                    this.formatterConfigKey,
                    this.includeFormatterConfigKeys,
                    this.availableFormatters
            );
            ConfigKeys.Key formatterKey = formatterKeys.get(this.formatterConfigKey);
            formatterKey.defaultValue(this.defaultFormat)
                    .importance(this.importance)
                    .width(this.width)
                    .displayName(this.displayName)
                    .group(this.groupName)
                    .addToDocumentation(this.documentation)
                    .validator(this.availableFormatters)
                    .recommender(this.availableFormatters);
            keys.getOrDefine(formatterKeys);
        }

        public void build(ConfigDef configDef) {
            Objects.requireNonNull(configDef);
            ConfigKeys keys = new ConfigKeys();
            this.build(keys);
            keys.addKeysTo(configDef, null);
        }
    }
}
