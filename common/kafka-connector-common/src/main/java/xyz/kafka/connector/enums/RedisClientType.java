package xyz.kafka.connector.enums;

/**
 * RedisType匹配
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public enum RedisClientType {
    /**
     * redis 类型
     */
    STANDALONE,
    CLUSTER,
    SENTINEL,
    MASTER_SLAVE,
}
