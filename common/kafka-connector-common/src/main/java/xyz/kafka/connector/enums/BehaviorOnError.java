package xyz.kafka.connector.enums;

/**
 * BehaviorOnError
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-09-25
 */
public enum BehaviorOnError {
    /**
     * 忽略
     */
    IGNORE,
    LOG,
    FAIL;
}
