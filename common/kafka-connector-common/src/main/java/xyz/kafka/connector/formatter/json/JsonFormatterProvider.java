package xyz.kafka.connector.formatter.json;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import xyz.kafka.connector.formatter.api.Formatter;
import xyz.kafka.connector.formatter.api.FormatterProvider;
import xyz.kafka.connector.utils.ConfigKeys;

import java.util.Map;

/**
 * JsonFormatterProvider
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class JsonFormatterProvider implements FormatterProvider {
    @Override
    public String name() {
        return JsonFormatter.NAME;
    }

    @Override
    public Class<JsonFormatter> formatterClass() {
        return JsonFormatter.class;
    }

    @Override
    public ConfigKeys configs() {
        return JsonFormatterConfig.configKeys();
    }

    @Override
    public Formatter create(Map<String, ?> properties) throws ConnectException, ConfigException {
        return new JsonFormatter(new JsonFormatterConfig(properties));
    }
}
