package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Pattern;

/**
 * RegexValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class RegexValidator extends Validators.SingleOrListValidator {
    private final Pattern pattern;

    public RegexValidator(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public void validate(String name, Object value) {
        if (!(value instanceof String str)) {
            throw new ConfigException(name, "Must be a string and cannot be null.");
        } else if (!this.pattern.matcher(str).matches()) {
            throw new ConfigException(name, value, String.format("must match pattern '%s'", this.pattern.pattern()));
        }
    }

    @Override
    public String toString() {
        return String.format("Matches regex %s", this.pattern.pattern());
    }
}
