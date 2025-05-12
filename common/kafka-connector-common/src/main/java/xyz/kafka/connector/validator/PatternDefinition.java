package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Objects;
import java.util.function.Function;

/**
 * PatternDefinition
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class PatternDefinition {
    private final ConfigDef.Validator validator;
    private final Function<String, Object> preResolvingParser;
    private final Function<String, String> variableNameExtractor;

    public PatternDefinition(ConfigDef.Validator validator,
                             Function<String, Object> preResolvingParser,
                             Function<String, String> variableNameExtractor) {
        this.validator = Objects.requireNonNull(validator);
        this.preResolvingParser = preResolvingParser != null ? preResolvingParser : s -> s;
        this.variableNameExtractor = variableNameExtractor != null ? variableNameExtractor : s -> s;
    }

    public ConfigDef.Validator validator() {
        return this.validator;
    }

    public Function<String, Object> preResolvingParser() {
        return this.preResolvingParser;
    }

    public Function<String, String> nameExtractor() {
        return this.variableNameExtractor;
    }
}
