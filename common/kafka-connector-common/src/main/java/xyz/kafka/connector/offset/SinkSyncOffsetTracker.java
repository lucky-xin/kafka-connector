package xyz.kafka.connector.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * It's a synchronous offset tracker to use with <code>FLUSH_SYNCHRONOUSLY_CONFIG=true</code>,
 * that will block on {@link SinkSyncOffsetTracker#offsets(Map)}.
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class SinkSyncOffsetTracker implements OffsetTracker<SinkRecord, TopicPartition, OffsetAndMetadata> {

    private final Function<Map<TopicPartition, OffsetAndMetadata>, Map<TopicPartition, OffsetAndMetadata>> offset;

    public SinkSyncOffsetTracker(
            Function<Map<TopicPartition, OffsetAndMetadata>, Map<TopicPartition, OffsetAndMetadata>> offset) {
        this.offset = offset;
    }

    @Override
    public SyncOffsetState pendingRecord(SinkRecord sr) {
        return new SyncOffsetState();
    }

    /**
     * This is a blocking method,
     * that blocks until client doesn't have any in-flight requests
     *
     * @param partitionAndOffset current offsets from a task
     * @return offsets to commit
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> offsets(Map<TopicPartition, OffsetAndMetadata> partitionAndOffset) {
        return offset.apply(partitionAndOffset);
    }
}
