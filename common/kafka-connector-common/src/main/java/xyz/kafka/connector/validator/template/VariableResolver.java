package xyz.kafka.connector.validator.template;

import java.util.Map;
import java.util.Objects;

/**
 * VariableResolver
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@FunctionalInterface
public interface VariableResolver<T> {
    Resolution<T> resolveVariable(String str, String str2, Object obj);

    static <T> VariableResolver<T> using(String variableName, T value) {
        return (varName, variable, parsed) -> varName.equals(variableName) ?
                Resolution.with(value) :
                Resolution.unresolved();
    }

    static <T> VariableResolver<T> using(Map<String, T> valuesByVariableName) {
        Objects.requireNonNull(valuesByVariableName);
        return (varName, variable, parsed) -> {
            if (valuesByVariableName.containsKey(varName)) {
                return Resolution.with(valuesByVariableName.get(varName));
            }
            return Resolution.unresolved();
        };
    }

    default VariableResolver<T> then(VariableResolver<T> secondary) {
        Objects.requireNonNull(secondary);
        return (varName, variable, parsed) -> {
            Resolution<T> result = this.resolveVariable(varName, variable, parsed);
            if (result.isResolved()) {
                return result;
            }
            return secondary.resolveVariable(varName, variable, parsed);
        };
    }
}
