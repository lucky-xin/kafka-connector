package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigException;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * DateTimeValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class DateTimeValidator extends Validators.SingleOrListValidator {
    private final Map<String, DateTimeFormatter> formats = new LinkedHashMap<>();

    @Override
    public void ensureValid(String str, Object obj) {
        super.ensureValid(str, obj);
    }

    public DateTimeValidator() {
        this.formats.put("yyyy-MM-dd", DateTimeFormatter.ISO_LOCAL_DATE);
        this.formats.put("yyyy-MM-dd'T'HH:mm:SS", DateTimeFormatter.ISO_DATE_TIME);
        this.formats.put("yyyy-'W'w-6EX", DateTimeFormatter.ISO_WEEK_DATE);
        this.formats.put("yyyy-MM-dd'T'HH:mm:SSV", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    public DateTimeValidator(String formatName, DateTimeFormatter format) {
        this.formats.put(Objects.requireNonNull(formatName), Objects.requireNonNull(format));
    }

    protected DateTimeValidator(DateTimeFormatter... formatters) {
        Arrays.stream(formatters).filter(Objects::nonNull).forEach(formatter -> {
            this.formats.put(formatter.toString(), formatter);
        });
        if (this.formats.isEmpty()) {
            throw new IllegalArgumentException("At least one non-null formatter must be specified");
        }
    }


    public DateTimeValidator(String... formats) {
        Arrays.stream(formats)
                .filter(Objects::nonNull)
                .forEach(format ->
                        this.formats.put(format, DateTimeFormatter.ofPattern(format))
                );
        if (this.formats.isEmpty()) {
            throw new IllegalArgumentException("At least one non-null format must be specified");
        }
    }

    @Override
    protected void validate(String name, Object value) {
        if (!(value instanceof String)) {
            throw new ConfigException(name, Objects.isNull(value) ? "Expected String, found Null" : "Expected String, but found " + value.getClass().getName());
        }
        boolean allFailed = true;
        for (DateTimeFormatter fmt : this.formats.values()) {
            try {
                fmt.parse((CharSequence) value);
                allFailed = false;
                break;
            } catch (DateTimeParseException ignored) {
            }
        }
        if (allFailed) {
            throw new ConfigException(String.format("'%s' having value '%s' must be of ISO 8601 format!", name, value));
        }
    }

    @Override
    public String toString() {
        if (this.formats.size() == 1) {
            return String.format("timestamp in format ``%s``", this.formats.keySet()
                    .stream().findFirst().get());
        }
        return String.format("timestamp in one of these formats: %s", this.formats.keySet()
                .stream().filter(Objects::nonNull)
                .collect(Collectors.joining("``, ``", "``", "``")));
    }
}
