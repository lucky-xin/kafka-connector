package xyz.kafka.connector.formatter.string;

import xyz.kafka.connector.formatter.api.Formatter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * StringFormatter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class StringFormatter implements Formatter {
    public static final String NAME = "string";
    private static final Logger log = LoggerFactory.getLogger(StringFormatter.class);
    private final StringFormatterConfig config;

    public StringFormatter(StringFormatterConfig config) {
        this.config = config;
        log.info("StringFormatter is configured. Record values will be converted to a UTF-8 String using org.apache.kafka.connect.data.Values.convertToString()");
    }

    @Override
    public byte[] formatValue(String topic, Schema schema, Object value) throws ConnectException {
        if (value != null) {
            return Values.convertToString(schema, value).getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }

    @Override
    public StringFormatterConfig config() {
        return this.config;
    }
}
