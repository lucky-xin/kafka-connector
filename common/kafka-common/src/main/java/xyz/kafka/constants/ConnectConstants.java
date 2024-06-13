package xyz.kafka.constants;

import org.apache.kafka.connect.data.Decimal;

/**
 * 常量池
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public interface ConnectConstants {

    String DECIMAL_PRECISION = "precision";
    String DECIMAL_SCALE = Decimal.SCALE_FIELD;
    /**
     * kafka消息头中key的class
     */
    String KEY_TYPE = "key.type";

    /**
     * kafka消息头中value的class
     */
    String VALUE_TYPE = "value.type";
}
