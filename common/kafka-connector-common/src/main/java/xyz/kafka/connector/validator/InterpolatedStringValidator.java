package xyz.kafka.connector.validator;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.ConfigException;
import xyz.kafka.connector.validator.template.Template;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * InterpolatedStringValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class InterpolatedStringValidator extends RegexValidator {
    public static final String NON_SUBSTITUTION_CHARACTERS = "[^\\\\${}]";
    public static final String ESCAPED_CHARACTERS = "\\\\.";
    private final String variableNamesList;
    private final Set<String> unquotedVariableNames;
    private final Map<Pattern, PatternDefinition> variableNamePatterns;
    private final Pattern replacePattern;

    private static Pattern getFormatStringPattern(Collection<String> unquotedVariableNames, Collection<String> variableNamePatterns, boolean tokenize) {
        return Pattern.compile("(?:[^\\\\${}]+|\\\\.|" + ("\\$\\{(?:" + join("|", unquotedVariableNames, variableNamePatterns, Pattern::quote) + ")}") + ")" + (tokenize ? "" : "*"));
    }

    private static String join(CharSequence joinChars, Collection<String> unquotedVariableNames, Collection<String> variableNamePatterns, Function<String, String> quoteFunction) {
        return String.join(joinChars, unquotedVariableNames.stream().map(quoteFunction).collect(Collectors.joining(joinChars)), String.join(joinChars, variableNamePatterns));
    }

    public static String removeVariableName(String value, String variableNamePlusPrefix, String suffix) {
        if (value.length() < variableNamePlusPrefix.length() + suffix.length()) {
            return value;
        }
        return value.substring(variableNamePlusPrefix.length(), value.length() - suffix.length());
    }

    public static String removeVariableNameAndBrackets(String value, String variableName) {
        return removeVariableName(value, variableName + "[", "]");
    }

    public static String removeSquareBrackets(String value) {
        int index = value.indexOf("[");
        if (index > 0) {
            return value.substring(0, index);
        }
        return value;
    }

    public InterpolatedStringValidator(Collection<String> unquotedVariableNames, Map<String, PatternDefinition> variableNamePatterns) {
        super(getFormatStringPattern(unquotedVariableNames, variableNamePatterns.keySet(), false));
        this.variableNamePatterns = variableNamePatterns.entrySet()
                .stream().map(Objects::requireNonNull)
                .collect(Collectors.toMap(
                        e -> Pattern.compile(e.getKey()),
                        Map.Entry::getValue)
                );
        this.variableNamesList = join(", ", unquotedVariableNames, variableNamePatterns.keySet(), v -> v);
        this.replacePattern = getFormatStringPattern(unquotedVariableNames, variableNamePatterns.keySet(), true);
        this.unquotedVariableNames = ImmutableSet.copyOf(new HashSet<>(unquotedVariableNames));
    }

    @Override
    public void validate(String name, Object value) {
        try {
            super.validate(name, value);
            this.variableNamePatterns.forEach((key, value1) -> validatePattern(name, name, key, value1));
        } catch (ConfigException e) {
            throw new ConfigException(name, "Must contain only valid variable substitutions or characters escaped with `\\`: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return String.format("optionally includes substitution(s): %s", this.variableNamesList);
    }

    protected void validatePattern(String name, String value, Pattern pattern, PatternDefinition validator) {
        Matcher matcher = pattern.matcher(value);
        while (matcher.find()) {
            validator.validator().ensureValid(name, matcher.group());
        }
    }

    public Template templateFor(String templateString) {
        validate("resolver input", templateString);
        Template.Builder builder = Template.builder();
        Matcher matcher = this.replacePattern.matcher(templateString);
        while (matcher.find()) {
            String replaced = matcher.group();
            if (replaced.startsWith("${") && replaced.endsWith("}") && replaced.length() > 3) {
                String variable = replaced.substring(2, replaced.length() - 1);
                if (this.unquotedVariableNames.contains(variable)) {
                    builder.addVariable(variable, variable, variable);
                } else {
                    Optional<PatternDefinition> patternDefn = matchingVariablePattern(variable);
                    patternDefn.ifPresent(defn ->
                            builder.addVariable(defn.nameExtractor().apply(variable), variable, defn.preResolvingParser().apply(variable)));
                }
            }
            builder.addLiteral(replaced);
        }
        return builder.build();
    }

    protected Optional<PatternDefinition> matchingVariablePattern(String str) {
        return this.variableNamePatterns.entrySet().stream()
                .filter(entry -> entry.getKey().matcher(str).matches())
                .map(Map.Entry::getValue).findFirst();
    }
}
