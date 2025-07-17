package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.enums.BehaviorOnError;
import xyz.kafka.connector.recommenders.EnumRecommender;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 日志字段格式转换
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class DateFormat<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(DateFormat.class);
    public static final String FIELDS = "fields";
    public static final String SOURCE_DATE_FORMAT = "source.date.format";
    public static final String TARGET_DATE_FORMAT = "target.date.format";
    public static final String SOURCE_DATE_TIME_ZONE = "source.date.time.zone";
    public static final String TARGET_DATE_TIME_ZONE = "target.date.time.zone";
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    ConfigDef.Importance.MEDIUM,
                    "Fields to convert.")
            .define(SOURCE_DATE_FORMAT,
                    ConfigDef.Type.STRING,
                    null,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM,
                    "Source date format.")
            .define(SOURCE_DATE_TIME_ZONE,
                    ConfigDef.Type.STRING,
                    "UTC",
                    ConfigDef.Importance.LOW,
                    "Source date time zone.")

            .define(TARGET_DATE_FORMAT,
                    ConfigDef.Type.STRING,
                    null,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM,
                    "Target date format.")
            .define(TARGET_DATE_TIME_ZONE,
                    ConfigDef.Type.STRING,
                    "UTC",
                    ConfigDef.Importance.LOW,
                    "target date time zone.")
            .define(BEHAVIOR_ON_ERROR,
                    ConfigDef.Type.STRING,
                    BehaviorOnError.LOG.name(),
                    new EnumRecommender<>(BehaviorOnError.class),
                    ConfigDef.Importance.LOW,
                    "behavior on error."
            );

    private List<String> fieldList;
    private DateTimeFormatter sourceTimeFormat;
    private DateTimeFormatter targetTimeFormat;
    private BehaviorOnError behaviorOnError;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldList = config.getList(FIELDS);
        ZoneId sourceZoneId = ZoneId.of(config.getString(SOURCE_DATE_TIME_ZONE));
        sourceTimeFormat = DateTimeFormatter.ofPattern(config.getString(SOURCE_DATE_FORMAT)).withZone(sourceZoneId);
        ZoneId targetZoneId = ZoneId.of(config.getString(TARGET_DATE_TIME_ZONE));
        targetTimeFormat = DateTimeFormatter.ofPattern(config.getString(TARGET_DATE_FORMAT)).withZone(targetZoneId);
        behaviorOnError = BehaviorOnError.valueOf(config.getString(BEHAVIOR_ON_ERROR));
    }

    @Override
    public R apply(R r) {
        Struct struct = Requirements.requireStructOrNull(r.value(), r.topic());
        if (struct == null) {
            return null;
        }
        Schema schema = r.valueSchema();
        Struct updatedValue = new Struct(schema);
        for (Field field : schema.fields()) {
            try {
                Object fieldValue = struct.get(field.name());
                if (!fieldList.contains(field.name())) {
                    updatedValue.put(field.name(), fieldValue);
                    continue;
                }
                if (!(fieldValue instanceof String text)) {
                    throw new IllegalStateException("field[" + field.name() + "] value type must be a string");
                }
                String parse = targetTimeFormat.format(sourceTimeFormat.parse(text));
                updatedValue.put(field.name(), parse);
            } catch (Exception e) {
                switch (behaviorOnError) {
                    case LOG -> log.error("convert field[" + field.name() + "] to timestamp error", e);
                    case FAIL ->
                            throw new ConnectException("convert field[" + field.name() + "] to timestamp error", e);
                    case IGNORE -> {

                    }
                    default -> {
                    }
                }
            }
        }
        return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema,
                updatedValue, r.timestamp(), r.headers());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // ooo
    }
}
