package xyz.kafka.connector.formatter.json;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.formatter.api.Formatter;

/**
 * JsonFormatter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class JsonFormatter implements Formatter {
    public static final String NAME = "json";
    private static final Logger log = LoggerFactory.getLogger(JsonFormatter.class);
    private final JsonFormatterConfig config;
    private final JsonConverter keyConverter = new JsonConverter();
    private final JsonConverter valueConverter = new JsonConverter();

    public JsonFormatter(JsonFormatterConfig config) {
        this.config = config;
        this.keyConverter.configure(config.values(), true);
        this.valueConverter.configure(config.values(), false);
        log.info("{} is configured", NAME);
    }

    @Override
    public byte[] formatKey(String topic, Schema schema, Object value) throws ConnectException {
        return this.keyConverter.fromConnectData(topic, schema, value);
    }

    public byte[] formatValue(String topic, Schema schema, Object value) throws ConnectException {
        return this.valueConverter.fromConnectData(topic, schema, value);
    }

    public JsonFormatterConfig config() {
        return this.config;
    }
}
