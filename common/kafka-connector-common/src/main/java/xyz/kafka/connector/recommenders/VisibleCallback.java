package xyz.kafka.connector.recommenders;

import java.util.Map;

/**
 * VisibleCallback
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@FunctionalInterface
public interface VisibleCallback {
    VisibleCallback ALWAYS_VISIBLE = (configItem, settings) -> true;

    boolean visible(String str, Map<String, Object> map);
}
