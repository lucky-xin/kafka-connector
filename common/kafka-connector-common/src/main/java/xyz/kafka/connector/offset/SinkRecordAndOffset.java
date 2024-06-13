package xyz.kafka.connector.offset;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * SinkRecordAndOffset
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-06
 */
public record SinkRecordAndOffset(SinkRecord sinkRecord, OffsetState offsetState) {

}