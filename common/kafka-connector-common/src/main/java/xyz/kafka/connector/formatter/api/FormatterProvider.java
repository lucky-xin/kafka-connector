package xyz.kafka.connector.formatter.api;

import xyz.kafka.connector.utils.ConfigKeys;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Map;

/**
 * FormatterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface FormatterProvider {
    Class<? extends Formatter> formatterClass();

    ConfigKeys configs();

    Formatter create(Map<String, ?> map) throws ConnectException, ConfigException;

    default String name() {
        return formatterClass().getName();
    }
}
