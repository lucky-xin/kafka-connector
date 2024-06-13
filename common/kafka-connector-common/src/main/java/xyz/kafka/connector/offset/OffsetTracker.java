package xyz.kafka.connector.offset;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Tracks processed records to calculate safe offsets to commit.
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public interface OffsetTracker<R, K, V> {

    /**
     * Method that return a total number of offset entries, that is in memory
     *
     * @return number of offset entries
     */
    default long numOffsetStateEntries() {
        return 0;
    }

    /**
     * Method that cleans up entries that are not needed anymore
     * (all the contiguous processed entries since the last reported offset)
     */
    default void updateOffsets() {
    }

    /**
     * Add a pending record
     *
     * @param r record that has to be added
     * @return offset state, associated with this record
     */
    OffsetState pendingRecord(R r);

    /**
     * Method that returns offsets, that are safe to commit
     *
     * @param partitionAndOffset current offsets, that are provided by a task
     * @return offsets that are safe to commit
     */
    Map<K, V> offsets(Map<K, V> partitionAndOffset);

    /**
     * Close partitions that are no longer assigned to the task
     *
     * @param partitions partitions that have to be closed
     */
    default void closePartitions(Collection<K> partitions) {
    }

    /**
     * get lowest watermark offset
     *
     * @return
     */
    default Optional<V> lowestWatermarkOffset() {
        return Optional.empty();
    }
}
