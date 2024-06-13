package xyz.kafka.connector.formatter.json;

import xyz.kafka.connector.formatter.api.FormatterConfig;
import xyz.kafka.connector.utils.ConfigKeys;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.DecimalFormat;
import xyz.kafka.connector.validator.Validators;

import java.util.Locale;
import java.util.Map;


/**
 * JsonFormatterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class JsonFormatterConfig extends FormatterConfig {
    private static final String GROUP_NAME = "JSON Formatter";
    public static final String SCHEMA_CACHE_SIZE_CONFIG = "schemas.cache.size";
    public static final String SCHEMA_CACHE_SIZE_DISPLAY = "Cache Size";
    public static final String SCHEMA_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in the JSON formatter.";
    public static final int SCHEMA_CACHE_SIZE_DEFAULT = 128;
    public static final String SCHEMA_ENABLE_CONFIG = "schemas.enable";
    public static final String SCHEMA_ENABLE_DISPLAY = "Schemas Enabled";
    public static final String SCHEMA_ENABLE_DOC = "Include schemas within each of the serialized values and keys.";
    public static final boolean SCHEMA_ENABLE_DEFAULT = false;
    public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
    private static final String DECIMAL_FORMAT_DOC = "Controls which format this converter will serialize decimals in. This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";
    private static final String DECIMAL_FORMAT_DISPLAY = "Decimal Format";
    private final int schemaCacheSize = getInt(SCHEMA_CACHE_SIZE_CONFIG);
    private final boolean enableSchemas = getBoolean(SCHEMA_ENABLE_CONFIG);
    private final DecimalFormat decimalFormat = DecimalFormat.valueOf(getString(DECIMAL_FORMAT_CONFIG).toUpperCase(Locale.ROOT));
    public static final String DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64.name();
    private static final ConfigDef CONFIG_DEF = configKeys().toConfigDef();

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    public static ConfigKeys configKeys() {
        ConfigKeys keys = new ConfigKeys();
        keys.define(SCHEMA_CACHE_SIZE_CONFIG, ConfigDef.Type.INT)
                .displayName(SCHEMA_CACHE_SIZE_DISPLAY)
                .defaultValue(SCHEMA_CACHE_SIZE_DEFAULT)
                .documentation(SCHEMA_CACHE_SIZE_DOC)
                .group(GROUP_NAME)
                .validator(Validators.between(0, 2048))
                .importance(ConfigDef.Importance.MEDIUM)
                .width(ConfigDef.Width.MEDIUM);
        keys.define(SCHEMA_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN)
                .displayName(SCHEMA_ENABLE_DISPLAY)
                .defaultValue(SCHEMA_ENABLE_DEFAULT)
                .documentation(SCHEMA_ENABLE_DOC)
                .group(GROUP_NAME)
                .importance(ConfigDef.Importance.MEDIUM)
                .width(ConfigDef.Width.MEDIUM);
        keys.define(DECIMAL_FORMAT_CONFIG, ConfigDef.Type.STRING)
                .displayName(DECIMAL_FORMAT_DISPLAY)
                .defaultValue(DECIMAL_FORMAT_DEFAULT)
                .validator(ConfigDef.CaseInsensitiveValidString.in(DecimalFormat.BASE64.name(), DecimalFormat.NUMERIC.name()))
                .documentation(DECIMAL_FORMAT_DOC)
                .group(GROUP_NAME)
                .importance(ConfigDef.Importance.MEDIUM)
                .width(ConfigDef.Width.MEDIUM);
        return keys;
    }

    public JsonFormatterConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public int schemaCacheSize() {
        return this.schemaCacheSize;
    }

    public boolean schemaEnable() {
        return this.enableSchemas;
    }

    public DecimalFormat decimalFormat() {
        return this.decimalFormat;
    }
}
