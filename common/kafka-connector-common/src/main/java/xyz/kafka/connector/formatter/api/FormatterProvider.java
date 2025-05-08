package xyz.kafka.connector.formatter.api;

import xyz.kafka.connector.utils.ConfigKeys;

import java.util.Map;

/**
 * FormatterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface FormatterProvider {
    /**
     * Formatter
     *
     * @return Class<?>
     */
    Class<? extends Formatter> formatterClass();

    /**
     * FormatterConfig
     *
     * @return ConfigKeys
     */
    ConfigKeys configs();

    /**
     * create Formatter
     *
     * @param map config
     * @return Formatter
     */
    Formatter create(Map<String, ?> map);

    /**
     * name
     *
     * @return String
     */
    default String name() {
        return formatterClass().getName();
    }
}
