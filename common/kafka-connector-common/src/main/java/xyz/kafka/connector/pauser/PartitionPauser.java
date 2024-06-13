package xyz.kafka.connector.pauser;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BooleanSupplier;

/**
 * PartitionPauser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-24
 */
public class PartitionPauser {
    private final Logger log = LoggerFactory.getLogger(PartitionPauser.class);
    private static final long PAUSE_POLL_TIMEOUT_MS = 100;

    private final SinkTaskContext context;
    private final BooleanSupplier pauseCondition;
    private final BooleanSupplier resumeCondition;
    private boolean partitionsPaused;

    public PartitionPauser(SinkTaskContext context,
                           BooleanSupplier pauseCondition,
                           BooleanSupplier resumeCondition) {
        this.context = context;
        this.pauseCondition = pauseCondition;
        this.resumeCondition = resumeCondition;
    }

    /**
     * Resume partitions if they are paused and resume condition is met.
     * Has to be run in the task thread.
     */
    public void maybeResumePartitions() {
        if (partitionsPaused) {
            if (resumeCondition.getAsBoolean()) {
                log.info("Resuming all partitions");
                context.resume(context.assignment().toArray(new TopicPartition[0]));
                partitionsPaused = false;
            } else {
                context.timeout(PAUSE_POLL_TIMEOUT_MS);
            }
        }
    }

    /**
     * Pause partitions if they are not paused and pause condition is met.
     * Has to be run in the task thread.
     */
    public void maybePausePartitions() {
        if (!partitionsPaused && pauseCondition.getAsBoolean()) {
            log.info("Pausing all partitions");
            context.pause(context.assignment().toArray(new TopicPartition[0]));
            context.timeout(PAUSE_POLL_TIMEOUT_MS);
            partitionsPaused = true;
        }
    }
}
