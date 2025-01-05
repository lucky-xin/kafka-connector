package xyz.kafka.connector.formatter.string;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import xyz.kafka.connector.formatter.api.Formatter;
import xyz.kafka.connector.formatter.api.FormatterProvider;
import xyz.kafka.connector.utils.ConfigKeys;

import java.util.Map;

/**
 * StringFormatterProvider
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class StringFormatterProvider implements FormatterProvider {
    @Override
    public String name() {
        return StringFormatter.NAME;
    }

    @Override
    public Class<StringFormatter> formatterClass() {
        return StringFormatter.class;
    }

    @Override
    public ConfigKeys configs() {
        return StringFormatterConfig.configKeys();
    }

    @Override
    public Formatter create(Map<String, ?> properties) throws ConnectException, ConfigException {
        return new StringFormatter(new StringFormatterConfig(properties));
    }
}
