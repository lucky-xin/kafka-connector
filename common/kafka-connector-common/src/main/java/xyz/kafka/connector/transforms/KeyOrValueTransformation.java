package xyz.kafka.connector.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * A transformation that work either a record key or record value.
 * <p>
 * The details of accessing and manipulating key/value are abstracted away.
 * Two imlementations (Key, Value) are provided as default methods.
 * This allows a new SMT to be written without duplicating the key/value specific code.
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
interface KeyOrValueTransformation<T extends ConnectRecord<T>> extends Transformation<T> {

    Object value(T t);

    Schema schema(T t);

    T newRecord(T t, Object object);

    T newRecord(T t, Object object, Schema schema);

    interface Key<T extends ConnectRecord<T>> extends KeyOrValueTransformation<T> {

        @Override
        default Object value(T t) {
            return t.key();
        }

        @Override
        default Schema schema(T t) {
            return t.keySchema();
        }

        @Override
        default T newRecord(T t, Object object) {
            return this.newRecord(t, object, t.keySchema());
        }

        @Override
        default T newRecord(T t, Object object, Schema schema) {
            return t.newRecord(t.topic(), t.kafkaPartition(), schema, object, t.valueSchema(), t.value(), t.timestamp(), t.headers());
        }
    }

    interface Value<T extends ConnectRecord<T>> extends KeyOrValueTransformation<T> {

        @Override
        default Object value(T t) {
            return t.value();
        }

        @Override
        default Schema schema(T t) {
            return t.valueSchema();
        }

        @Override
        default T newRecord(T t, Object object) {
            return this.newRecord(t, object, t.valueSchema());
        }

        @Override
        default T newRecord(T t, Object object, Schema schema) {
            return t.newRecord(t.topic(), t.kafkaPartition(), t.keySchema(), t.key(), schema, object, t.timestamp(), t.headers());
        }
    }
}
