package xyz.kafka.enums;

/**
 * 数据库类型
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-12
 */
public enum DbType {
    /**
     * 未知数据库
     */
    NONE,

    /**
     * elasticsearch
     */
    ES,

    /**
     * MySQL
     */
    MY_SQL,


    /**
     * MongoDB
     */
    MONGO_DB;

    public static DbType from(String val) {
        if (val == null) {
            return DbType.NONE;
        }
        for (DbType value : DbType.values()) {
            if (value.name().equals(val)) {
                return value;
            }
        }
        return DbType.NONE;
    }
}
