package xyz.kafka.connector.formatter.string;

import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.connector.formatter.api.FormatterConfig;
import xyz.kafka.connector.utils.ConfigKeys;

import java.util.Map;

/**
 * StringFormatterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class StringFormatterConfig extends FormatterConfig {
    private static final ConfigDef CONFIG_DEF = configKeys().toConfigDef();

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    public static ConfigKeys configKeys() {
        return new ConfigKeys();
    }

    public StringFormatterConfig(Map<?, ?> originals) {
        super(config(), originals);
    }
}
