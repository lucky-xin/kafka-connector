package xyz.kafka.connector.validator;

import cn.hutool.core.text.CharSequenceUtil;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import xyz.kafka.connector.utils.Strings;

import java.net.InetAddress;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Validators
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class Validators {
    public static ComposeableValidator use(final ConfigDef.Validator validator) {
        return validator instanceof ComposeableValidator v ? v : new ComposeableValidator() {
            @Override
            public void ensureValid(String name, Object value) {
                validator.ensureValid(name, value);
            }

            @Override
            public String toString() {
                return validator.toString();
            }
        };
    }

    protected static ComposeableValidator singleOrListOfStrings(final ConfigDef.Validator validator) {
        return validator instanceof SingleOrListValidator v ? v : new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                validator.ensureValid(name, value);
            }
        };
    }

    public static ComposeableValidator lengthBetween(int minLength, int maxLength) {
        return new StringLengthValidator(minLength, maxLength);
    }

    public static ComposeableValidator regexPattern() {
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value != null && !value.toString().trim().isEmpty()) {
                    try {
                        Pattern.compile(value.toString());
                    } catch (PatternSyntaxException var4) {
                        throw new ConfigException(name, value, String.format("must be a valid regular expression pattern: %s", var4.getMessage()));
                    }
                } else {
                    throw new ConfigException(name, value, "May not be blank");
                }
            }

            @Override
            public String toString() {
                return "valid Java regular expression pattern";
            }
        };
    }

    public static ComposeableValidator blackOrRegexPattern() {
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value != null && !value.toString().trim().isEmpty()) {
                    try {
                        Pattern.compile(value.toString());
                    } catch (PatternSyntaxException var4) {
                        throw new ConfigException(name, value, String.format("must be a valid regular expression pattern: %s", var4.getMessage()));
                    }
                }
            }

            @Override
            public String toString() {
                return "valid Java regular expression pattern";
            }
        };
    }

    public static ComposeableValidator pattern(String pattern) {
        return pattern(Pattern.compile(pattern));
    }

    public static ComposeableValidator pattern(Pattern pattern) {
        return new RegexValidator(pattern);
    }

    public static ComposeableValidator atLeast(Number min) {
        return use(ConfigDef.Range.atLeast(min));
    }

    public static ComposeableValidator between(Number min, Number max) {
        return use(ConfigDef.Range.between(min, max));
    }

    public static ComposeableValidator parsedAs(ConfigDef.Type type) {
        return new StringAsTypeValidator(type, null);
    }

    public static ComposeableValidator parsedAs(ConfigDef.Type type, ConfigDef.Validator validator) {
        return new StringAsTypeValidator(type, validator);
    }

    public static ComposeableValidator notNull() {
        return use(new ConfigDef.NonNullValidator());
    }

    public static ComposeableValidator nonEmptyString() {
        return singleOrListOfStrings(new ConfigDef.NonEmptyString());
    }

    public static ComposeableValidator blank() {
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value == null || !value.toString().trim().isEmpty()) {
                    throw new ConfigException(name, value, "Must be blank");
                }
            }

            @Override
            public String toString() {
                return "blank";
            }
        };
    }

    public static ComposeableValidator nullOrBlank() {
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value != null && !value.toString().trim().isEmpty()) {
                    throw new ConfigException(name, value, "Must be blank or null");
                }
            }

            @Override
            public String toString() {
                return "blank or null";
            }
        };
    }

    public static ComposeableValidator beginsWith(final String prefix) {
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value == null || !value.toString().startsWith(prefix)) {
                    throw new ConfigException(name, value, "Must begin with '" + prefix + "'");
                }
            }

            @Override
            public String toString() {
                return "begins with '" + prefix + "'";
            }
        };
    }

    public static <E extends Enum<E>> ComposeableValidator oneOf(Class<E> enumClass) {
        return oneOfLowercase(enumClass);
    }

    public static <T, V> ComposeableValidator oneOf(Iterable<T> allowedValues, Function<T, V> mapping) {
        Set<V> values = StreamSupport.stream(allowedValues.spliterator(), false).map(mapping).collect(Collectors.toSet());
        return oneOf(values);
    }

    public static ComposeableValidator oneOf(Iterable<?> allowedValues) {
        final Set<?> values = ImmutableSet.copyOf(allowedValues);
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value == null || !values.contains(value)) {
                    throw new ConfigException(name, value, "Must be one of " + values);
                }
            }

            @Override
            public String toString() {
                return values.size() == 1 ? values.iterator().next().toString() : "one of " + values;
            }
        };
    }

    public static ComposeableValidator oneOf(String... allowedValues) {
        return oneOf(Arrays.stream(allowedValues).filter(Objects::nonNull).collect(Collectors.toSet()));
    }

    public static ComposeableValidator emptyOrIn(String... allowedValues) {
        Set<String> collect = Arrays.stream(allowedValues).filter(Objects::nonNull).collect(Collectors.toSet());
        collect.add("");
        return oneOf(collect);
    }

    public static <E extends Enum<E>> ComposeableValidator oneOf(Class<E> enumClass, Function<String, String> conversion) {
        return oneOf(Arrays.stream(enumClass.getEnumConstants())
                .filter(Objects::nonNull).map(Enum::name)
                .map(conversion)
                .collect(Collectors.toSet()));
    }

    public static <E extends Enum<E>> ComposeableValidator oneOfLowercase(Class<E> enumClass) {
        return oneOf(enumClass, String::toLowerCase);
    }

    public static <E extends Enum<E>> ComposeableValidator oneOfUppercase(Class<E> enumClass) {
        return oneOf(enumClass, String::toUpperCase);
    }

    public static ComposeableValidator oneOfAnyCase(String... allowedValues) {
        return oneStringOf(Arrays.asList(allowedValues), false);
    }

    public static <T> ComposeableValidator oneStringOf(Iterable<T> allowedValues, boolean matchCase, Function<T, String> mapping) {
        Set<String> values = StreamSupport.stream(allowedValues.spliterator(), false)
                .map(mapping)
                .collect(Collectors.toSet());
        return oneStringOf(values, matchCase);
    }

    public static ComposeableValidator oneStringOf(Iterable<String> allowedValues, boolean matchCase) {
        final Set<String> values = ImmutableSet.copyOf(allowedValues);
        final Predicate<String> matcher;
        if (matchCase) {
            matcher = values::contains;
        } else {
            matcher = v -> values.stream().anyMatch(s -> s.equalsIgnoreCase(v));
        }

        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value == null || !matcher.test(value.toString())) {
                    throw new ConfigException(name, value, "Must be one of " + values);
                }
            }

            @Override
            public String toString() {
                return "one of " + values;
            }
        };
    }

    public static ComposeableValidator anyOf(ConfigDef.Validator... validators) {
        final List<ConfigDef.Validator> allValidators = Arrays.stream(validators)
                .filter(Objects::nonNull)
                .toList();
        if (allValidators.size() == 1) {
            return use(allValidators.get(0));
        } else if (allValidators.size() < 2) {
            throw new IllegalArgumentException("Must specify at least two validators");
        } else {
            return new ComposeableValidator() {
                @Override
                public void ensureValid(String name, Object value) {
                    if (value instanceof List<?> values) {
                        for (Object v : values) {
                            validate(name, v);
                        }
                    } else {
                        validate(name, value);
                    }

                }

                public void validate(String name, Object value) {

                    for (ConfigDef.Validator validator : allValidators) {
                        try {
                            validator.ensureValid(name, value);
                            return;
                        } catch (Exception ignored) {
                        }
                    }

                    throw new ConfigException(name, value, "Must be " + this);
                }

                @Override
                public String toString() {
                    return Strings.readableJoin(", or ", allValidators);
                }
            };
        }
    }

    public static ComposeableValidator allOf(ConfigDef.Validator... validators) {
        final List<ConfigDef.Validator> allValidators = Arrays.stream(validators)
                .filter(Objects::nonNull)
                .toList();
        if (allValidators.size() == 1) {
            return use(allValidators.get(0));
        } else if (allValidators.size() < 2) {
            throw new IllegalArgumentException("Must specify at least two validators");
        } else {
            return new ComposeableValidator() {
                @Override
                public void ensureValid(String name, Object value) {
                    for (ConfigDef.Validator validator : allValidators) {
                        validator.ensureValid(name, value);
                    }

                }

                @Override
                public String toString() {
                    return Strings.readableJoin(", and ", allValidators);
                }
            };
        }
    }

    public static ComposeableValidator instanceOf(final Class<?> clazz) {
        Objects.requireNonNull(clazz);
        return new ComposeableValidator() {
            @Override
            public void ensureValid(String name, Object value) {
                if (!(value instanceof Class) || !clazz.isAssignableFrom((Class<?>) value)) {
                    throw new ConfigException(name, value, "Class must extend: " + clazz.getName());
                }
            }

            @Override
            public String toString() {
                return "Any class implementing: " + clazz.getName();
            }
        };
    }

    public static ComposeableValidator nonEmptyList() {
        return NonEmptyListValidator.INSTANCE;
    }

    public static ComposeableValidator validUri(String... schemes) {
        return new UriValidator(schemes);
    }

    public static ComposeableValidator portNumber() {
        return between(1, 65535);
    }

    public static ComposeableValidator hostnameOrIpAddress() {
        return new SingleOrListValidator() {
            @Override
            protected void validate(String name, Object value) {
                if (value != null && !value.toString().isEmpty()) {
                    try {
                        InetAddress.getByName(value.toString());
                    } catch (Exception var4) {
                        throw new ConfigException(name, value, "Host cannot be resolved, or IP address is not valid");
                    }
                } else {
                    throw new ConfigException(name, value, "Must not be empty");
                }
            }

            @Override
            public String toString() {
                return "Valid hostname or IP address";
            }
        };
    }

    public static ComposeableValidator timeZoneValidator() {
        return TimeZoneValidator.INSTANCE;
    }

    public static ComposeableValidator dateTimeValidator() {
        return new DateTimeValidator();
    }

    public static ComposeableValidator dateTimeValidator(String name, DateTimeFormatter format) {
        return new DateTimeValidator(name, format);
    }

    public static ComposeableValidator dateTimeValidator(String... formats) {
        return new DateTimeValidator(formats);
    }

    public static ComposeableValidator mapValidator(ConfigDef.Validator keyValidator, ConfigDef.Validator valueValidator) {
        return new MapValidator(keyValidator, valueValidator);
    }

    public static ComposeableValidator topicValidator() {
        return new TopicNameValidator();
    }

    public static ComposeableValidator fieldNameValidator() {
        return new RegexValidator(Pattern.compile("[A-Za-z_][A-Za-z0-9_]*")) {
            @Override
            public String toString() {
                return "Valid field name (" + super.toString() + ")";
            }
        };
    }

    public static ComposeableValidator dateTimeFormatValidator() {
        return new DateTimeFormatValidator();
    }

    public static ComposeableValidator optionalVariables(String... variables) {
        return (new OptionalVariablesBuilder()).withVariableNames(variables).build();
    }

    public static OptionalVariablesBuilder optionalVariables() {
        return new OptionalVariablesBuilder();
    }

    public static TransformingValidatorBuilder first() {
        return new TransformingValidatorBuilder();
    }

    public static class OptionalVariablesBuilder {
        private final Collection<String> unquotedVariableNames = new ArrayList<>();
        private final Map<String, PatternDefinition> variableNamePatterns = new HashMap<>();

        public OptionalVariablesBuilder withVariableNames(String... variables) {
            Arrays.stream(variables).forEach(this::withVariableName);
            return this;
        }

        public OptionalVariablesBuilder withVariableName(String variable) {
            if (CharSequenceUtil.isEmpty(Objects.requireNonNull(variable))) {
                throw new IllegalArgumentException("The variable name may not be blank");
            } else {
                this.unquotedVariableNames.add(variable);
                return this;
            }
        }

        public OptionalVariablesBuilder withVariableNamePatterns(String... patterns) {
            Arrays.stream(patterns).forEach(this::withVariableNamePattern);
            return this;
        }

        public OptionalVariablesBuilder withVariableNamePattern(String pattern) {
            return this.withVariableNamePattern(pattern, Validators.notNull());
        }

        public OptionalVariablesBuilder withVariableNamePattern(String pattern, ConfigDef.Validator validator) {
            return this.withVariableNamePattern(pattern, validator, null, null);
        }

        public OptionalVariablesBuilder withVariableNamePattern(String pattern, ConfigDef.Validator validator, Function<String, Object> parser, Function<String, String> variableNameExtractor) {
            PatternDefinition patternDefn = new PatternDefinition(validator, parser, variableNameExtractor);
            this.variableNamePatterns.put(Objects.requireNonNull(pattern), patternDefn);
            return this;
        }

        public OptionalVariablesBuilder withMapVariable(String variableName, ConfigDef.Validator keyValidator, ConfigDef.Validator valueValidator) {
            return this.withMapVariable(variableName, keyValidator, valueValidator, (s) -> s, (s) -> s);
        }

        public OptionalVariablesBuilder withMapVariable(String variableName, ConfigDef.Validator keyValidator, ConfigDef.Validator valueValidator, Function<String, ?> keyParser, Function<String, ?> valueParser) {
            Objects.requireNonNull(keyValidator);
            Objects.requireNonNull(valueValidator);
            Objects.requireNonNull(keyParser);
            Objects.requireNonNull(valueParser);
            ConfigDef.Validator mapValidator = Validators.first().transform("as map", (k, v) -> {
                return InterpolatedStringValidator.removeVariableName(v.toString(), variableName + "[", "]");
            }).then(Validators.mapValidator(keyValidator, valueValidator));
            String mapPattern = variableName + "\\[([^\\]]*)\\]";
            Function<String, Object> parser = (str) -> {
                String entries = InterpolatedStringValidator.removeVariableName(str, variableName + "[", "]");
                return MapValidator.parseMap(entries, keyParser, valueParser);
            };
            Function<String, String> nameExtractor = InterpolatedStringValidator::removeSquareBrackets;
            return this.withVariableNamePattern(mapPattern, mapValidator, parser, nameExtractor);
        }

        public OptionalVariablesBuilder withTimestampVariable(String variableName) {
            return this.withTimestampVariable(variableName, "UTC,yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }

        public OptionalVariablesBuilder withTimestampVariable(String variableName, String defaultFormat) {
            Objects.requireNonNull(defaultFormat);
            String pattern = variableName + "\\[([^\\]]*)\\]";
            Function<String, Object> parser = str -> {
                String format = InterpolatedStringValidator.removeVariableNameAndBrackets(str, variableName);
                if (format.trim().isEmpty()) {
                    format = defaultFormat;
                }

                return DateTimeFormatValidator.parseDateTimeFormat(format);
            };
            Function<String, String> extractVariable =
                    str -> InterpolatedStringValidator.removeVariableNameAndBrackets(str, variableName);
            ConfigDef.Validator formatValidator = Validators.first()
                    .transformStrings("timestamp format", extractVariable)
                    .then(Validators.dateTimeFormatValidator().orBlank());
            Function<String, String> nameExtractor = InterpolatedStringValidator::removeSquareBrackets;
            return this.withVariableNamePattern(pattern, formatValidator, parser, nameExtractor);
        }

        public InterpolatedStringValidator build() {
            if (this.unquotedVariableNames.isEmpty() && this.variableNamePatterns.isEmpty()) {
                throw new IllegalArgumentException("At least one variable name or pattern must be specified");
            } else {
                return new InterpolatedStringValidator(this.unquotedVariableNames, this.variableNamePatterns);
            }
        }
    }

    protected abstract static class SingleOrListValidator implements ComposeableValidator {
        protected SingleOrListValidator() {
        }

        @Override
        public void ensureValid(String name, Object value) {
            if (value instanceof List<?> values) {
                for (Object v : values) {
                    if (v == null) {
                        validate(name, null);
                    } else {
                        validate(name, v);
                    }
                }
            } else {
                validate(name, value);
            }

        }

        protected abstract void validate(String var1, Object var2);
    }

    public static class TransformingValidatorBuilder {
        private Map<String, BiFunction<String, Object, Object>> transforms = new LinkedHashMap<>();

        public TransformingValidatorBuilder() {
        }

        public TransformingValidatorBuilder and() {
            return this;
        }

        public TransformingValidatorBuilder trim() {
            return this.transformStrings("trimming", String::trim);
        }

        public TransformingValidatorBuilder lowercase() {
            return this.transformStrings("lowercase", String::toLowerCase);
        }

        public TransformingValidatorBuilder uppercase() {
            return this.transformStrings("uppercase", String::toUpperCase);
        }

        public TransformingValidatorBuilder transformStrings(String name, Function<String, String> function) {
            return this.transform(name, (k, v) -> v instanceof String ? function.apply((String) v) : v);
        }

        public TransformingValidatorBuilder transform(String name, BiFunction<String, Object, Object> function) {
            this.transforms.put(Objects.requireNonNull(name), Objects.requireNonNull(function));
            return this;
        }

        public ComposeableValidator then(final ConfigDef.Validator validator) {
            Objects.requireNonNull(validator);
            return this.transforms.isEmpty() && validator instanceof ComposeableValidator c ? c : new ComposeableValidator() {
                @Override
                public void ensureValid(String name, Object value) {
                    Object v = value;
                    BiFunction<String, Object, Object> transform;
                    for (Iterator<BiFunction<String, Object, Object>> var4 = TransformingValidatorBuilder.this.transforms.values().iterator(); var4.hasNext(); v = transform.apply(name, v)) {
                        transform = var4.next();
                    }

                    validator.ensureValid(name, v);
                }

                @Override
                public String toString() {
                    String transformNames = String.join(", ", TransformingValidatorBuilder.this.transforms.keySet());
                    return "after " + transformNames + " must be " + validator;
                }
            };
        }
    }

    public interface ComposeableValidator extends ConfigDef.Validator {
        default ComposeableValidator or(ConfigDef.Validator other) {
            return Validators.anyOf(this, other);
        }

        default ComposeableValidator and(ConfigDef.Validator other) {
            return Validators.allOf(this, other);
        }

        default ComposeableValidator orNullOrBlank() {
            return this.or(Validators.nullOrBlank());
        }

        default ComposeableValidator orBlank() {
            return this.or(Validators.blank());
        }
    }
}
