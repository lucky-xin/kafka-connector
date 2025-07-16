package xyz.kafka.connector.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;

import java.util.Map;

/**
 * Drops a message key or value based on the given ECMAScript predicate.
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class Drop<T extends ConnectRecord<T>> extends ScriptingTransformation<T> {

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
    }

    @Override
    protected T doApply(T t) {
        if (negative) {
            return Boolean.FALSE.equals(engine.eval(t, Boolean.class)) ? null : t;
        }
        return Boolean.TRUE.equals(engine.eval(t, Boolean.class)) ? null : t;
    }

}
