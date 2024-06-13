package xyz.kafka.connector.formatter.api;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * FormatterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public abstract class FormatterConfig extends AbstractConfig {
    protected FormatterConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals);
    }
}
