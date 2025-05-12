package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

/**
 * StringLengthValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class StringLengthValidator extends Validators.SingleOrListValidator {
    private final int minLength;
    private final int maxLength;

    public StringLengthValidator(int minLength, int maxLength) {
        if (minLength > maxLength) {
            throw new IllegalArgumentException("Minimum length may not be larger than maximum length");
        }
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    @Override
    protected void validate(String name, Object value) {
        if (!(value instanceof String str)) {
            throw new ConfigException(name, "Must be a string and cannot be null.");
        }
        if (str.length() < this.minLength || str.length() > this.maxLength) {
            throw new ConfigException(name, String.format("'%s' must have no fewer than %d and no more than %d characters",
                    value, this.minLength, this.maxLength));
        }
    }

    @Override
    public String toString() {
        return String.format("Must be from %d through %d characters long", this.minLength, this.maxLength);
    }
}
