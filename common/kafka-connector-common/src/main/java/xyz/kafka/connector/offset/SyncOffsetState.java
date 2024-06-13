package xyz.kafka.connector.offset;

/**
 * SyncOffsetState
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-04-01
 */
public class SyncOffsetState implements OffsetState {
    @Override
    public void markProcessed() {
        // document why this method is empty
    }

    @Override
    public boolean isProcessed() {
        return false;
    }

    @Override
    public long offset() {
        return -1;
    }
}
