package xyz.kafka.connector.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * A transformation that work either a record key or record value.
 * <p>
 * The details of accessing and manipulating key/value are abstracted away.
 * Two implementations (Key, Value) are provided as default methods.
 * This allows a new SMT to be written without duplicating the key/value specific code.
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
interface KeyOrValueTransformation<T extends ConnectRecord<T>> extends Transformation<T> {

    /**
     * 获取给定对象的值
     *
     * @param t 传入的对象
     * @return 对象的值
     */
    Object value(T t);

    /**
     * 获取给定对象的架构
     *
     * @param t 传入的对象
     * @return 对象的架构
     */
    Schema schema(T t);

    /**
     * 创建一个新的记录，使用给定对象和新值
     *
     * @param t      传入的对象
     * @param object 新的值
     * @return 新创建的记录
     */
    T newRecord(T t, Object object);

    /**
     * 创建一个新的记录，使用给定对象、新值和指定的架构
     *
     * @param t      传入的对象
     * @param object 新的值
     * @param schema 新记录的架构
     * @return 新创建的记录
     */
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
