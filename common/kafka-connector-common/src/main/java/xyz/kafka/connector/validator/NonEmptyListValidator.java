package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

import java.util.List;

/**
 * MapValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class NonEmptyListValidator implements Validators.ComposeableValidator {
    public static final NonEmptyListValidator INSTANCE = new NonEmptyListValidator();

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null || ((List<?>) value).isEmpty()) {
            throw new ConfigException(name, value, "Empty list");
        }
    }

    @Override
    public String toString() {
        return "Non-empty list";
    }
}
