package xyz.kafka.connector.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * It's an asynchronous implementation of <code>OffsetTracker</code>
 *
 * <p>Since ElasticsearchClient can potentially process multiple batches asynchronously for the same
 * partition, if we don't want to wait for all in-flight batches at the end of the put call
 * (or flush/preCommit) we need to keep track of what's the highest offset that is safe to commit.
 * For now, we do that at the individual record level because batching is handled by BulkProcessor,
 * and we don't have control over grouping/ordering.
 */
public class SinkAsyncOffsetTracker implements OffsetTracker<SinkRecord, TopicPartition, OffsetAndMetadata> {

    private static final Logger log = LoggerFactory.getLogger(SinkAsyncOffsetTracker.class);

    private final Map<TopicPartition, TreeSet<OffsetState>> offsetsByPartition = new HashMap<>();

    private final AtomicLong numEntries = new AtomicLong();
    private final SinkTaskContext context;

    public SinkAsyncOffsetTracker(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Partitions are no longer owned, we should release all related resources.
     *
     * @param topicPartitions partitions to close
     */
    @Override
    public void closePartitions(Collection<TopicPartition> topicPartitions) {
        topicPartitions.forEach(tp -> {
            TreeSet<OffsetState> offsets = offsetsByPartition.remove(tp);
            if (offsets != null) {
                numEntries.getAndAdd(-offsets.size());
            }
        });
    }

    @Override
    public Optional<OffsetAndMetadata> lowestWatermarkOffset() {
        return offsetsByPartition.values()
                .stream()
                .map(TreeSet::first)
                .min(Comparator.comparing(OffsetState::offset))
                .map(t -> new OffsetAndMetadata(t.offset() + 1));
    }

    /**
     * This method assumes that new records are added in offset order.
     * Older records can be re-added, and the same Offset object will be return if its
     * offset hasn't been reported yet.
     *
     * @param sinkRecord record to add
     * @return offset state record that can be used to mark the record as processed
     */
    @Override
    public OffsetState pendingRecord(SinkRecord sinkRecord) {
        log.trace("Adding pending record");
        TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
        if (!context.assignment().contains(tp)) {
            String msg = String.format("Found a topic name '%s' that doesn't match assigned partitions."
                    + " Connector doesn't support topic mutating SMTs", sinkRecord.topic());
            throw new ConnectException(msg);
        }
        TreeSet<OffsetState> offsetStates = offsetsByPartition
                // Insertion order needs to be maintained
                .computeIfAbsent(tp, key -> new TreeSet<>(Comparator.comparing(OffsetState::offset)));
        AsyncOffsetState offsetState = new AsyncOffsetState(sinkRecord.kafkaOffset());
        OffsetState last = null;
        boolean label = !offsetStates.isEmpty()
                && (last = offsetStates.last()) != null
                && sinkRecord.kafkaOffset() > last.offset();
        if (label) {
            offsetStates.add(offsetState);
            numEntries.incrementAndGet();
        }
        return offsetState;
    }

    /**
     * @return overall number of entries currently in memory.
     */
    @Override
    public long numOffsetStateEntries() {
        return numEntries.get();
    }

    /**
     * Move offsets to the highest we can.
     */
    @Override
    public void updateOffsets() {
        log.trace("Updating offsets");
        offsetsByPartition.values()
                .stream()
                .parallel()
                .forEach(c -> c.stream()
                        .filter(OffsetState::isProcessed)
                        .forEach(x -> numEntries.decrementAndGet())
                );
        log.trace("Updated offsets, num entries: {}", numEntries);
    }

    /**
     * @param partitionAndOffset current offsets from a task
     * @return offsets to commit
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> offsets(Map<TopicPartition, OffsetAndMetadata> partitionAndOffset) {
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>(offsetsByPartition.size());
        for (Map.Entry<TopicPartition, TreeSet<OffsetState>> entry : offsetsByPartition.entrySet()) {
            Iterator<OffsetState> itr = entry.getValue().iterator();
            TopicPartition tp = entry.getKey();
            while (itr.hasNext()) {
                OffsetState state = itr.next();
                if (state.isProcessed()) {
                    itr.remove();
                    offset.put(tp, new OffsetAndMetadata(state.offset() + 1));
                } else {
                    break;
                }
            }
        }
        return offset;
    }
}