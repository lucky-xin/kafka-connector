package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

import java.time.DateTimeException;
import java.time.ZoneId;

/**
 * TimeZoneValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class TimeZoneValidator extends Validators.SingleOrListValidator {
    public static final TimeZoneValidator INSTANCE = new TimeZoneValidator();

    @Override
    protected void validate(String name, Object value) {
        if (value != null) {
            try {
                ZoneId.of(value.toString());
            } catch (DateTimeException e) {
                throw new ConfigException(name, value, String.format("'%s' is not a known time zone identifier", value));
            }
        }
    }

    @Override
    public String toString() {
        return "Valid timezone name or identifier";
    }
}
