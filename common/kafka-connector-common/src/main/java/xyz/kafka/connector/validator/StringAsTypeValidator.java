package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Objects;

/**
 * StringAsTypeValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class StringAsTypeValidator extends Validators.SingleOrListValidator {
    private final ConfigDef.Type type;
    private final ConfigDef.Validator typeValidator;

    public StringAsTypeValidator(ConfigDef.Type type, ConfigDef.Validator typeValidator) {
        this.type = Objects.requireNonNull(type);
        this.typeValidator = typeValidator;
    }

    @Override
    protected void validate(String name, Object value) {
        if (!(value instanceof String)) {
            throw new ConfigException(name, "Must be a non-null string.");
        }
        Object typedValue = ConfigDef.parseType(name, value, this.type);
        if (this.typeValidator != null) {
            this.typeValidator.ensureValid(name, typedValue);
        }
    }

    @Override
    public String toString() {
        return this.type.toString().toLowerCase();
    }
}
