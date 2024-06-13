package xyz.kafka.connector.formatter.api;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Formatter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface Formatter {
    byte[] formatValue(String str, Schema schema, Object obj) throws ConnectException;

    FormatterConfig config();

    default byte[] formatKey(SinkRecord sr) throws ConnectException {
        try {
            return formatKey(sr.topic(), sr.keySchema(), sr.key());
        } catch (ConnectException e) {
            throw new ConnectException(String.format("Unable to serialize record from the %s Kafka Topic (Partition: %d, Offset: %d)", sr.topic(), sr.kafkaPartition(), sr.kafkaOffset()), e);
        }
    }

    default byte[] formatKey(String topic, SchemaAndValue schemaAndValue) throws ConnectException {
        return formatKey(topic, schemaAndValue.schema(), schemaAndValue.value());
    }

    default byte[] formatKey(String topic, Schema schema, Object value) throws ConnectException {
        return formatValue(topic, schema, value);
    }

    default byte[] formatValue(SinkRecord sr) throws ConnectException {
        try {
            return formatValue(sr.topic(), sr.valueSchema(), sr.value());
        } catch (ConnectException e) {
            throw new ConnectException(String.format("Unable to serialize record from the %s Kafka Topic (Partition: %d, Offset: %d)", sr.topic(), sr.kafkaPartition(), sr.kafkaOffset()), e);
        }
    }

    default byte[] formatValue(String topic, SchemaAndValue schemaAndValue) throws ConnectException {
        return formatValue(topic, schemaAndValue.schema(), schemaAndValue.value());
    }
}
