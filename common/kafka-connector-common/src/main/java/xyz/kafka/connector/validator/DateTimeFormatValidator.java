package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;

/**
 * DateTimeFormatValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class DateTimeFormatValidator extends Validators.SingleOrListValidator {
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final ZoneId DEFAULT_TIME_ZONE = ZoneId.of("UTC");

    public static DateTimeFormatter parseDateTimeFormat(String value) {
        String formatStr;
        Objects.requireNonNull(value);
        String[] parts = value.split(",");
        Optional<ZoneId> zoneId = parseTimeZone(parts[0]);
        if (zoneId.isEmpty()) {
            formatStr = parts[0].trim();
            if (formatStr.isEmpty()) {
                formatStr = DEFAULT_FORMAT;
            }
        } else if (parts.length == 2) {
            formatStr = parts[1].trim();
        } else {
            formatStr = DEFAULT_FORMAT;
        }
        try {
            return DateTimeFormatter.ofPattern(formatStr).withZone(zoneId.orElse(DEFAULT_TIME_ZONE));
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    protected static Optional<ZoneId> parseTimeZone(String id) {
        try {
            return Optional.of(ZoneId.of(id.trim()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    protected void validate(String name, Object value) {
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ConfigException(name, value, "Must not be null or blank");
        }
        try {
            parseDateTimeFormat(value.toString());
        } catch (Exception e) {
            throw new ConfigException(name, value, "Must follow the Java SimpleDateFormat rules");
        }
    }

    @Override
    public String toString() {
        return "``<format>`` or ``<zoneId>,<format>`` or ``<zoneId>``, " +
                "where ``<zoneId>`` is the ID of the time zone in which the timestamps are to be output, and ``<format>`` is the Java DateTimeFormatter format pattern";
    }
}
