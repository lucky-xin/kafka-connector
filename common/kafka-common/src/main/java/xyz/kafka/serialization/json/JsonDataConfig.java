package xyz.kafka.serialization.json;

import cn.hutool.core.util.StrUtil;
import io.confluent.connect.schema.AbstractDataConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.json.DecimalFormat;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JsonSchemaDataConfig
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class JsonDataConfig extends AbstractDataConfig {

    public static final String OBJECT_ADDITIONAL_PROPERTIES_CONFIG = "object.additional.properties";
    public static final boolean OBJECT_ADDITIONAL_PROPERTIES_DEFAULT = true;
    public static final String OBJECT_ADDITIONAL_PROPERTIES_DOC =
            "Whether to allow additional properties for object schemas.";

    public static final String USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG = "use.optional.for.nonrequired";
    public static final boolean USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT = true;
    public static final String USE_OPTIONAL_FOR_NON_REQUIRED_DOC =
            "Whether to set non-required properties to be optional.";

    public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
    public static final String DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64.name();
    private static final String DECIMAL_FORMAT_DOC =
            "Controls which format this converter will serialize decimals in."
                    + " This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";

    public static final String DATE_TIME_FORMAT_CONFIG = "date.time.format";
    public static final String DATE_TIME_FORMAT_DEFAULT = null;
    private static final String DATE_TIME_FORMAT_DOC = "json data date time field format string";

    public static final String DATE_FORMAT_CONFIG = "date.format";
    public static final String DATE_FORMAT_DEFAULT = null;
    private static final String DATE_FORMAT_DOC = "json data date field format string";

    public static final String TIME_FORMAT_CONFIG = "time.format";
    public static final String TIME_FORMAT_DEFAULT = null;
    private static final String TIME_FORMAT_DOC = "json data time field format string";

    public static final String ZONE_ID_CONFIG = "zone.id";
    public static final String ZONE_ID_DEFAULT = "UTC+08:00";
    private static final String ZONE_ID_DOC = "json data date field format zone id, eg: UTC,UTC+08:00" +
            "default is UTC+08:00 ";

    public static ConfigDef baseConfigDef() {
        return AbstractDataConfig.baseConfigDef()
                .define(
                        OBJECT_ADDITIONAL_PROPERTIES_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        OBJECT_ADDITIONAL_PROPERTIES_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        OBJECT_ADDITIONAL_PROPERTIES_DOC
                ).define(
                        USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        USE_OPTIONAL_FOR_NON_REQUIRED_DOC
                ).define(
                        DECIMAL_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        DECIMAL_FORMAT_DEFAULT,
                        CaseInsensitiveValidString.in(
                                DecimalFormat.BASE64.name(),
                                DecimalFormat.NUMERIC.name()),
                        ConfigDef.Importance.LOW,
                        DECIMAL_FORMAT_DOC
                ).define(
                        DATE_TIME_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        DATE_TIME_FORMAT_DEFAULT,
                        ConfigDef.Importance.LOW,
                        DATE_TIME_FORMAT_DOC
                ).define(
                        DATE_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        DATE_FORMAT_DEFAULT,
                        ConfigDef.Importance.LOW,
                        DATE_FORMAT_DOC
                ).define(
                        TIME_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        TIME_FORMAT_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TIME_FORMAT_DOC
                ).define(
                        ZONE_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        ZONE_ID_DEFAULT,
                        ConfigDef.Importance.LOW,
                        ZONE_ID_DOC)
                ;
    }

    private final Optional<DateTimeFormatter> dateTimeFormatter;
    private final Optional<DateTimeFormatter> dateFormatter;
    private final Optional<DateTimeFormatter> timeFormatter;

    private final DecimalFormat decimalFormat;
    private final ZoneId zoneId;

    public JsonDataConfig(Map<?, ?> props) {
        super(baseConfigDef(), props);
        this.zoneId = ZoneId.of(getString(ZONE_ID_CONFIG));
        this.dateTimeFormatter = Optional.ofNullable(getString(DATE_TIME_FORMAT_CONFIG))
                .filter(StrUtil::isNotEmpty)
                .map(t -> DateTimeFormatter.ofPattern(t, Locale.ROOT).withZone(zoneId));
        this.dateFormatter = Optional.ofNullable(getString(DATE_FORMAT_CONFIG))
                .filter(StrUtil::isNotEmpty)
                .map(t -> DateTimeFormatter.ofPattern(t, Locale.ROOT).withZone(zoneId));
        this.timeFormatter = Optional.ofNullable(getString(TIME_FORMAT_CONFIG))
                .filter(StrUtil::isNotEmpty)
                .map(t -> DateTimeFormatter.ofPattern(t, Locale.ROOT).withZone(zoneId));
        this.decimalFormat = DecimalFormat.valueOf(getString(DECIMAL_FORMAT_CONFIG).toUpperCase(Locale.ROOT));

    }

    public boolean allowAdditionalProperties() {
        return getBoolean(OBJECT_ADDITIONAL_PROPERTIES_CONFIG);
    }

    public boolean useOptionalForNonRequiredProperties() {
        return getBoolean(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG);
    }

    /**
     * Get the serialization format for decimal types.
     *
     * @return the decimal serialization format
     */
    public DecimalFormat decimalFormat() {
        return this.decimalFormat;
    }

    public Optional<DateTimeFormatter> dateTimeFormatter() {
        return this.dateTimeFormatter;
    }

    public Optional<DateTimeFormatter> dateFormatter() {
        return this.dateFormatter;
    }

    public Optional<DateTimeFormatter> timeFormatter() {
        return this.timeFormatter;
    }

    public ZoneId zoneId() {
        return this.zoneId;
    }

    public static class Builder {

        private final Map<String, Object> props = new HashMap<>();

        public Builder with(String key, Object value) {
            props.put(key, value);
            return this;
        }

        public JsonDataConfig build() {
            return new JsonDataConfig(props);
        }
    }

    public static class CaseInsensitiveValidString implements ConfigDef.Validator {

        final Set<String> validStrings;

        private CaseInsensitiveValidString(List<String> validStrings) {
            this.validStrings = validStrings.stream()
                    .map(s -> s.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet());
        }

        public static CaseInsensitiveValidString in(String... validStrings) {
            return new CaseInsensitiveValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s == null || !validStrings.contains(s.toUpperCase(Locale.ROOT))) {
                throw new ConfigException(name, o, "String must be one of (case insensitive): "
                        + String.join(", ", validStrings));
            }
        }

        @Override
        public String toString() {
            return "(case insensitive) [" + String.join(", ", validStrings) + "]";
        }
    }

}
