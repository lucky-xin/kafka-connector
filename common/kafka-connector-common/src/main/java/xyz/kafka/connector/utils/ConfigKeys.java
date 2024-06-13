package xyz.kafka.connector.utils;

import cn.hutool.core.text.CharSequenceUtil;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ConfigKeys
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class ConfigKeys implements Iterable<ConfigKeys.Key> {
    private final Map<String, Key> keysByName = new LinkedHashMap<>();

    public class Key {
        private final String name;
        private String displayName;
        private ConfigDef.Validator validator;
        private ConfigDef.Recommender recommender;
        private String documentation = "";
        private String group = "";
        private int orderInGroup = -1;
        private Object defaultValue = ConfigDef.NO_DEFAULT_VALUE;
        private ConfigDef.Type type = ConfigDef.Type.STRING;
        private ConfigDef.Importance importance = ConfigDef.Importance.HIGH;
        private ConfigDef.Width width = ConfigDef.Width.NONE;
        private List<String> dependents = new ArrayList<>();
        private boolean internalConfig = false;

        protected Key(String name) {
            Objects.requireNonNull(name);
            ConfigKeys.requireNotBlank(name);
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        public Key rename(String newName) {
            return ConfigKeys.this.rename(name(), newName);
        }

        public ConfigDef.Type type() {
            return this.type;
        }

        public Key type(ConfigDef.Type type) {
            Objects.requireNonNull(type);
            this.type = type;
            return this;
        }

        public String documentation() {
            return this.documentation;
        }

        public Key documentation(String doc) {
            this.documentation = doc != null ? doc.trim() : "";
            return this;
        }

        public Key addToDocumentation(String doc) {
            if (CharSequenceUtil.isEmpty(this.documentation)) {
                documentation(doc);
            } else if (CharSequenceUtil.isNotEmpty(doc)) {
                this.documentation = this.documentation.trim() + " " + doc.trim();
            }
            return this;
        }

        public boolean hasDefaultValue() {
            return this.defaultValue != ConfigDef.NO_DEFAULT_VALUE;
        }

        public Object defaultValue() {
            return this.defaultValue;
        }

        public Key defaultValue(Object value) {
            this.defaultValue = value;
            return this;
        }

        public Key noDefaultValue() {
            return defaultValue(ConfigDef.NO_DEFAULT_VALUE);
        }

        public ConfigDef.Importance importance() {
            return this.importance;
        }

        public Key importance(ConfigDef.Importance importance) {
            this.importance = importance != null ? importance : ConfigDef.Importance.HIGH;
            return this;
        }

        public ConfigDef.Validator validator() {
            return this.validator;
        }

        public Key validator(ConfigDef.Validator validator) {
            this.validator = validator;
            return this;
        }

        public ConfigDef.Recommender recommender() {
            return this.recommender;
        }

        public Key recommender(ConfigDef.Recommender recommender) {
            this.recommender = recommender;
            return this;
        }

        public boolean hasGroup() {
            return CharSequenceUtil.isNotEmpty(group());
        }

        public boolean hasGroup(String groupName) {
            if (CharSequenceUtil.isEmpty(groupName)) {
                return !hasGroup();
            }
            return groupName.equals(group());
        }

        public String group() {
            return this.group;
        }

        public Key group(String group) {
            this.group = group;
            return this;
        }

        public ConfigDef.Width width() {
            return this.width;
        }

        public Key width(ConfigDef.Width width) {
            this.width = width != null ? width : ConfigDef.Width.NONE;
            return this;
        }

        public String displayName() {
            return this.displayName != null ? this.displayName : name();
        }

        public Key displayName(String displayName) {
            this.displayName = CharSequenceUtil.isEmpty(displayName) ? null : displayName;
            return this;
        }

        public List<String> dependents() {
            return this.dependents;
        }

        public Key dependents(String... dependentNames) {
            return dependents(new ArrayList<>(Arrays.asList(dependentNames)));
        }

        public Key dependents(List<String> dependentNames) {
            this.dependents = dependentNames;
            return this;
        }

        public Key dependents(Key... dependents) {
            dependents().clear();
            if (!(dependents == null || dependents.length == 0)) {
                for (Key dependent : dependents) {
                    addDependent(dependent);
                }
            }
            return this;
        }

        public Key addDependent(Key dependent) {
            if (dependent != null) {
                addDependent(dependent.name());
            }
            return this;
        }

        public Key addDependent(String dependentName) {
            if (CharSequenceUtil.isNotEmpty(dependentName) && !this.dependents.contains(dependentName) && !name().equals(dependentName)) {
                this.dependents.add(dependentName);
            }
            return this;
        }

        public Key addDependents(Key... dependents) {
            for (Key dependent : dependents) {
                if (dependent != null) {
                    addDependent(dependent.name());
                }
            }
            return this;
        }

        public Key addDependents(String... dependentNames) {
            return addDependents(List.of(dependentNames));
        }

        public Key addDependents(List<String> dependentNames) {
            if (dependentNames != null && !dependentNames.isEmpty()) {
                if (this.dependents == null || this.dependents.isEmpty()) {
                    this.dependents = dependentNames;
                } else {
                    dependentNames.forEach(this::addDependent);
                }
            }
            return this;
        }

        public Key dependsUpon(Key otherKey) {
            if (!(otherKey == null || otherKey == this)) {
                otherKey.addDependent(this);
            }
            return this;
        }

        public Key dependsUpon(String otherKey) {
            if (!CharSequenceUtil.isEmpty(otherKey) && ConfigKeys.this.keysByName.containsKey(otherKey)) {
                return dependsUpon(ConfigKeys.this.get(otherKey));
            }
            throw new IllegalArgumentException("The '" + otherKey + "' does not exist");
        }

        public boolean isDependentUpon(String otherKey) {
            if (CharSequenceUtil.isEmpty(otherKey)) {
                return false;
            }
            return dependents().contains(otherKey);
        }

        public boolean removeDependent(String otherKey) {
            if (CharSequenceUtil.isEmpty(otherKey)) {
                return false;
            }
            return dependents().remove(otherKey);
        }

        public boolean removeDependent(Key otherKey) {
            return removeDependent(otherKey);
        }

        public boolean isInternal() {
            return this.internalConfig;
        }

        public Key internal(boolean internal) {
            this.internalConfig = internal;
            return this;
        }

        public int orderInGroup() {
            return this.orderInGroup;
        }

        public Key orderInGroup(int orderInGroup) {
            this.orderInGroup = orderInGroup;
            return this;
        }

        public Key copy(Key other) {
            type(other.type()).defaultValue(other.defaultValue()).validator(other.validator()).importance(other.importance()).documentation(other.documentation()).group(other.group()).orderInGroup(other.orderInGroup()).width(other.width()).displayName(other.displayName).dependents(other.dependents()).recommender(other.recommender()).internal(other.isInternal());
            return this;
        }

        public ConfigKeys end() {
            return ConfigKeys.this;
        }

        protected ConfigDef.ConfigKey buildWith(int orderInGroup) {
            return new ConfigDef.ConfigKey(name(), type(), defaultValue(), validator(), importance(), documentation(), group(), orderInGroup, width(), displayName(), new ArrayList(dependents()), recommender(), isInternal());
        }
    }

    public ConfigKeys() {
    }

    public ConfigKeys(ConfigDef configDef) {
        Objects.requireNonNull(configDef);
        defineAll(configDef);
    }

    public synchronized ConfigKeys defineAll(ConfigDef configDef) {
        Objects.requireNonNull(configDef);
        configDef.configKeys().values().forEach(this::define);
        return this;
    }

    public synchronized ConfigKeys defineAll(ConfigKeys other) {
        Objects.requireNonNull(other);
        other.forEach(key -> define(key.name).copy(key));
        return this;
    }

    public synchronized Key define(ConfigDef.ConfigKey configKey) {
        Objects.requireNonNull(configKey.name);
        requireNotBlank(configKey.name);
        return define(configKey.name, configKey.type)
                .defaultValue(configKey.defaultValue)
                .validator(configKey.validator)
                .importance(configKey.importance)
                .documentation(configKey.documentation)
                .group(configKey.group)
                .orderInGroup(configKey.orderInGroup)
                .width(configKey.width)
                .displayName(configKey.displayName)
                .addDependents(configKey.dependents)
                .recommender(configKey.recommender)
                .internal(configKey.internalConfig);
    }

    public synchronized Key define(String name) {
        Objects.requireNonNull(name);
        requireNotBlank(name);
        return this.keysByName.computeIfAbsent(name, Key::new);
    }

    public Key define(String name, ConfigDef.Type type) {
        return define(name).type(type);
    }

    public synchronized Key get(String name) {
        Objects.requireNonNull(name);
        return this.keysByName.get(name);
    }

    public synchronized Key getOrDefine(Key key) {
        if (key == null) {
            return null;
        }
        String keyName = key.name();
        Key existingKey = get(keyName);
        if (existingKey == null) {
            return define(keyName).copy(key);
        }
        if (existingKey != key) {
            existingKey.copy(key);
        }
        return existingKey;
    }

    public synchronized List<Key> getOrDefine(Iterable<Key> configKeys) {
        Objects.requireNonNull(configKeys);
        List<Key> result = new ArrayList<>();
        if (configKeys != this) {
            configKeys.forEach(k -> result.add(getOrDefine(k)));
        }
        return result;
    }

    public synchronized boolean contains(String name) {
        Objects.requireNonNull(name);
        return this.keysByName.containsKey(name);
    }

    public synchronized boolean remove(String name) {
        Objects.requireNonNull(name);
        return this.keysByName.remove(name) != null;
    }

    public synchronized ConfigKeys prefixKeyNames(String prefix) {
        Objects.requireNonNull(prefix);
        return renameAll(keyName -> prefix + keyName);
    }

    public synchronized ConfigKeys renameAll(Function<String, String> renameFunction) {
        Objects.requireNonNull(renameFunction);
        Map<String, Key> newKeys = new LinkedHashMap<>();
        Map<String, String> oldKeyNamesByNew = new HashMap<>();
        for (Map.Entry<String, Key> entry : this.keysByName.entrySet()) {
            Key key = entry.getValue();
            String oldName = key.name();
            String newName = renameFunction.apply(oldName);
            if (newName == null || oldName.equals(newName)) {
                newKeys.put(oldName, key);
            } else {
                newKeys.put(newName, new Key(newName).copy(key));
                oldKeyNamesByNew.put(newName, oldName);
            }
        }
        this.keysByName.clear();
        this.keysByName.putAll(newKeys);
        stream().forEach(k -> oldKeyNamesByNew.forEach((newKeyName, value) -> {
            if (k.removeDependent(value)) {
                k.addDependent(newKeyName);
            }
        }));
        return this;
    }

    public synchronized Key rename(String oldName, String newName) {
        Key oldKey = get(oldName);
        if (oldKey == null) {
            throw new IllegalArgumentException("The key with the old name '" + oldName + "' does not exist");
        } else if (oldName.equals(newName)) {
            return oldKey;
        } else {
            if (get(newName) != null) {
                throw new IllegalArgumentException("A key with the new name '" + newName + "' already exists");
            }
            renameAll(keyName -> {
                if (oldName.equals(keyName)) {
                    return newName;
                }
                return null;
            });
            return get(newName);
        }
    }

    public ConfigKeys alter(Consumer<Key> alterFunction, Predicate<Key> predicate) {
        Objects.requireNonNull(alterFunction);
        Objects.requireNonNull(predicate);
        this.keysByName.values().stream().filter(predicate).forEach(alterFunction);
        return this;
    }

    public ConfigKeys alter(Consumer<Key> alterFunction) {
        return alter(alterFunction, k -> true);
    }

    public int size() {
        return this.keysByName.size();
    }

    public boolean isEmpty() {
        return this.keysByName.isEmpty();
    }

    @Override // java.lang.Iterable
    public Iterator<Key> iterator() {
        return this.keysByName.values().iterator();
    }

    @Override // java.lang.Iterable
    public Spliterator<Key> spliterator() {
        return this.keysByName.values().spliterator();
    }

    public Stream<Key> stream() {
        return this.keysByName.values().stream();
    }

    public Set<String> keyNames() {
        return this.keysByName.keySet();
    }

    public Set<String> groups() {
        return this.keysByName.values().stream()
                .filter(Key::hasGroup)
                .map(Key::group).collect(Collectors.toSet());
    }

    public Stream<Key> keysInGroup(String groupName) {
        return this.keysByName.values().stream().filter(k -> k.hasGroup(groupName));
    }

    public ConfigDef toConfigDef() {
        return addKeysTo(new ConfigDef(), null);
    }

    public ConfigDef addKeysTo(ConfigDef configDef, Map<String, Key> keysAlreadyInConfigDef) {
        Objects.requireNonNull(configDef);
        Map<String, AtomicInteger> keyCountsByGroupName = new HashMap<>();
        Set<String> existingKeyNames = new HashSet<>();
        configDef.configKeys().values().forEach(key -> {
            existingKeyNames.add(key.name);
            keyCountsByGroupName.computeIfAbsent(key.name, n -> new AtomicInteger(0)).incrementAndGet();
        });
        this.keysByName.values().forEach(key -> {
            if (!existingKeyNames.contains(key.name())) {
                configDef.define(key.buildWith(keyCountsByGroupName.computeIfAbsent(key.group(), g -> new AtomicInteger(0)).incrementAndGet()));
            } else if (keysAlreadyInConfigDef != null) {
                keysAlreadyInConfigDef.put(key.name(), key);
            }
        });
        return configDef;
    }

    protected static void requireNotBlank(String name) {
        if (CharSequenceUtil.isEmpty(name)) {
            throw new IllegalArgumentException("The name may not be composed of only whitespace");
        }
    }
}
