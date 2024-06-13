package xyz.kafka.connector.offset;

/**
 * AsyncOffsetState
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-04-01
 */
public class AsyncOffsetState implements OffsetState {

    private final long offset;
    private volatile boolean processed;

    public AsyncOffsetState(long offset) {
        this.offset = offset;
    }

    @Override
    public void markProcessed() {
        processed = true;
    }

    @Override
    public boolean isProcessed() {
        return processed;
    }

    @Override
    public long offset() {
        return offset;
    }
}