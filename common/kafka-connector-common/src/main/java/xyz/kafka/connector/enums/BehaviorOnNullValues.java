package xyz.kafka.connector.enums;

/**
 * BehaviorOnNullValues
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2025-07-11
 */
public enum BehaviorOnNullValues {

    /**
     * Ignore the null value and continue processing the rest of the batch.
     */
    IGNORE,
    DELETE,
    FAIL
}
