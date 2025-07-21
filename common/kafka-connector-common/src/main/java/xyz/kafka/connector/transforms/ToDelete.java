package xyz.kafka.connector.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * 根基于给定的ECMAScript表达式，删除消息value
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class ToDelete<T extends ConnectRecord<T>> extends ScriptingTransformation<T> {

    @Override
    protected T doApply(T t) {
        if (negative) {
            return Boolean.FALSE.equals(engine.eval(t, Boolean.class)) ? newRecord(t) : t;
        }
        return Boolean.TRUE.equals(engine.eval(t, Boolean.class)) ? newRecord(t) : t;
    }

    private T newRecord(T t) {
        return t.newRecord(t.topic(), t.kafkaPartition(), t.keySchema(), t.key(), null, null, t.timestamp(), t.headers());
    }
}
