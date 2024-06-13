package xyz.kafka.connector.recommenders;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ValidValuesCallback
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@FunctionalInterface
public interface ValidValuesCallback {
    ValidValuesCallback EMPTY = (configItem, settings) -> Collections.emptyList();


    List<Object> validValues(String str, Map<String, Object> map);
}
