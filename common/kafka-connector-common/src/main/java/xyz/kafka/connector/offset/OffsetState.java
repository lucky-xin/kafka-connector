package xyz.kafka.connector.offset;

import org.jetbrains.annotations.NotNull;

/**
 * OffsetState
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public interface OffsetState extends Comparable<OffsetState> {

    /**
     * Marks the offset as processed (ready to report to preCommit)
     */
    void markProcessed();

    /**
     * isProcessed
     * @return true if record is processed
     */
    boolean isProcessed();

    /**
     * offset
     * @return current offset
     */
    long offset();

    /**
     * compareTo
     *
     * @param o the object to be compared.
     * @return
     */
    @Override
    default int compareTo(@NotNull OffsetState o) {
        return (int) (this.offset() - o.offset());
    }
}
