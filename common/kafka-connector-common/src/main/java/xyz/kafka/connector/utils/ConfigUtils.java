package xyz.kafka.connector.utils;

import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.connector.enums.BehaviorOnNullValues;
import xyz.kafka.connector.recommenders.EnumRecommender;

/**
 * ConfigUtils
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2025-07-11
 */
public class ConfigUtils {

    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
            + "non-null key and a null value (i.e. Kafka tombstone records). Valid options are "
            + "'ignore', 'delete', and 'fail'.";
    private static final String BEHAVIOR_ON_NULL_VALUES_DISPLAY = "Behavior for null-valued records";
    private static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = BehaviorOnNullValues.FAIL.name().toLowerCase();

    public static void behaviorOnNullValuesConfig(ConfigDef configDef, String group, int order) {
        configDef.define(
                BEHAVIOR_ON_NULL_VALUES_CONFIG,
                ConfigDef.Type.STRING,
                BEHAVIOR_ON_NULL_VALUES_DEFAULT,
                new EnumRecommender<>(BehaviorOnNullValues.class),
                ConfigDef.Importance.LOW,
                BEHAVIOR_ON_NULL_VALUES_DOC,
                group,
                order,
                ConfigDef.Width.SHORT,
                BEHAVIOR_ON_NULL_VALUES_DISPLAY,
                new EnumRecommender<>(BehaviorOnNullValues.class)
        );
    }
}
