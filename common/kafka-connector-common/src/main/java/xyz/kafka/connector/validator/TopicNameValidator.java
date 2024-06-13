package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Pattern;

/**
 * TopicNameValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class TopicNameValidator extends Validators.SingleOrListValidator {
    public static final int MIN_LENGTH = 1;
    public static final int MAX_LENGTH = 249;
    private static final Pattern PATTERN = Pattern.compile("[a-zA-Z0-9+._\\-]{1,249}");

    @Override
    protected void validate(String name, Object value) {
        if (!(value instanceof String)) {
            throw new ConfigException(name, "Must be a string and cannot be null.");
        }
        String topicName = value.toString();
        if (topicName.isEmpty()) {
            throw new ConfigException(name, value, "topic names may not be empty");
        } else if (topicName.length() > 249) {
            throw new ConfigException(name, value, String.format("topic names may not be longer than %d characters", Integer.valueOf((int) MAX_LENGTH)));
        } else if (!PATTERN.matcher(value.toString()).matches()) {
            throw new ConfigException(name, value, "topic names may have 1-249 ASCII alphanumeric, `+`, `.`, `_`, and `-` characters");
        }
    }

    @Override
    public String toString() {
        return "Valid topic names contain 1-249 ASCII alphanumeric, ``+``, ``.``, ``_`` and ``-`` characters";
    }
}
