package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kafka.connector.config.TimeConfig;
import xyz.kafka.connector.enums.BehaviorOnError;
import xyz.kafka.connector.enums.TimeTranslator;
import xyz.kafka.connector.validator.Validators;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

/**
 * 日期转换成时间戳转换器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class TimestampTransform<R extends ConnectRecord<R>> extends AbstractTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(TimestampTransform.class);
    private static final String PURPOSE = "converting timestamp formats";
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";

    private BehaviorOnError behaviorOnError;
    private TimeConfig config;

    protected TimestampTransform() {
        super(new ConfigDef()
                .define(TimeConfig.FIELDS,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.MEDIUM,
                        "Fields to convert."
                ).define(TimeConfig.SOURCE_TYPE,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Validators.oneOfUppercase(TimeTranslator.class),
                        ConfigDef.Importance.HIGH,
                        "The desired timestamp representation: STRING, UNIX, DATE, TIME, BIG_DECIMAL or TIMESTAMP")
                .define(TimeConfig.SOURCE_DATE_FORMAT,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "Source date format."
                ).define(TimeConfig.SOURCE_TIME_ZONE,
                        ConfigDef.Type.STRING,
                        "UTC",
                        ConfigDef.Importance.LOW,
                        "Source date time zone,default: UTC."
                ).define(TimeConfig.SOURCE_UNIX_PRECISION,
                        ConfigDef.Type.STRING,
                        TimeConfig.UNIX_PRECISION_MILLIS,
                        ConfigDef.Importance.LOW,
                        "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. " +
                                "Used to generate the output when type=UNIX or used to parse the input if the input is a Long." +
                                "Note: This SMT will cause precision loss during conversions from, and to, values with sub-millisecond components."
                ).define(TimeConfig.TARGET_TYPE,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Validators.oneOfUppercase(TimeTranslator.class),
                        ConfigDef.Importance.HIGH,
                        "The desired timestamp representation: STRING, UNIX, DATE, TIME, BIG_DECIMAL or TIMESTAMP"
                ).define(TimeConfig.TARGET_DATE_FORMAT,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "Target date format."
                ).define(TimeConfig.TARGET_TIME_ZONE,
                        ConfigDef.Type.STRING,
                        "UTC",
                        ConfigDef.Importance.LOW,
                        "Target time zone,default: UTC."
                ).define(TimeConfig.TARGET_UNIX_PRECISION,
                        ConfigDef.Type.STRING,
                        TimeConfig.UNIX_PRECISION_MILLIS,
                        ConfigDef.Importance.LOW,
                        "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. " +
                                "Used to generate the output when type=UNIX or used to parse the input if the input is a Long." +
                                "Note: This SMT will cause precision loss during conversions from, and to, values with sub-millisecond components."
                ).define(BEHAVIOR_ON_ERROR,
                        ConfigDef.Type.STRING,
                        BehaviorOnError.IGNORE.name(),
                        Validators.oneOfUppercase(BehaviorOnError.class),
                        ConfigDef.Importance.LOW,
                        "behavior on error."
                )
        );
    }

    @Override
    protected void configure(Map<String, ?> configs, AbstractConfig abstractConfig) {
        behaviorOnError = BehaviorOnError.valueOf(abstractConfig.getString(BEHAVIOR_ON_ERROR));
        config = new TimeConfig(abstractConfig);
    }

    @Override
    public R apply(R r) {
        if (schema(r) == null) {
            return applySchemaless(r);
        } else {
            return applyWithSchema(r);
        }
    }

    private R applyWithSchema(R r) {
        Schema schema = schema(r);
        if (config.getTimeFields().isEmpty()) {
            Object value = value(r);
            // New schema is determined by the requested target timestamp type
            Schema updatedSchema = config.getTargetType().typeSchema(schema.isOptional());
            return newRecord(r, convertTimestamp(value, timestampTypeFromSchema(schema)),
                    updatedSchema);
        } else {
            Struct value = requireStructOrNull(value(r), PURPOSE);
            Schema updatedSchema;
            if (isSchemaCache) {
                updatedSchema = schemaCache.get(r.topic(), k -> getSchema(schema));
            } else {
                updatedSchema = getSchema(schema);
            }
            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(r, updatedValue, updatedSchema);
        }
    }

    private Schema getSchema(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (config.getTimeFields().contains(field.name())) {
                builder.field(field.name(), config.getTargetType().typeSchema(field.schema().isOptional()));
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        if (schema.isOptional()) {
            builder.optional();
        }
        if (schema.defaultValue() != null) {
            Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
            builder.defaultValue(updatedDefaultValue);
        }

        return builder.build();
    }

    private R applySchemaless(R r) {
        Object rawValue = value(r);
        if (rawValue == null || config.getTimeFields().isEmpty()) {
            return newRecord(r, null);
        } else {
            Map<String, Object> value = requireMap(rawValue, PURPOSE);
            HashMap<String, Object> updatedValue = new HashMap<>(value);
            for (String field : config.getTimeFields()) {
                Object o = value.get(field);
                if (o == null) {
                    updatedValue.put(field, null);
                    continue;
                }
                TimeTranslator translator = inferTimestampType(o);
                updatedValue.put(field, convertTimestamp(o, translator));
            }
            return newRecord(r, updatedValue, null);
        }
    }

    private Struct applyValueWithSchema(Struct struct, Schema updatedSchema) {
        if (struct == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : struct.schema().fields()) {
            final Object updatedFieldValue;
            if (config.getTimeFields().contains(field.name())) {
                updatedFieldValue = convertTimestamp(struct.get(field), timestampTypeFromSchema(field.schema()));
            } else {
                updatedFieldValue = struct.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private Object convertTimestamp(Object timestamp, TimeTranslator sourceTranslator) {
        if (timestamp == null) {
            return null;
        }
        Instant instant = null;
        try {
            instant = sourceTranslator.toInstant(config, timestamp);
        } catch (Exception e) {
            switch (behaviorOnError) {
                case FAIL -> throw new ConnectException("Could not convert timestamp from:" + timestamp);
                case LOG -> log.error("Could not convert timestamp from:" + timestamp, e);
                case IGNORE -> {
                }
                default -> {
                }
            }
        }
        if (instant == null) {
            return null;
        }
        TimeTranslator targetTranslator = config.getTargetType();
        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type null");
        }
        return targetTranslator.toTarget(config, instant);
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private TimeTranslator inferTimestampType(Object timestamp) {
        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (timestamp instanceof Date) {
            return TimeTranslator.TIMESTAMP;
        } else if (timestamp instanceof Long) {
            return TimeTranslator.UNIX;
        } else if (timestamp instanceof String) {
            return TimeTranslator.STRING;
        }
        throw new DataException("TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps");
    }

    private TimeTranslator timestampTypeFromSchema(Schema schema) {
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TimeTranslator.TIMESTAMP;
        } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return TimeTranslator.DATE;
        } else if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TimeTranslator.TIME;
        } else if (schema.type().equals(Schema.Type.STRING)) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TimeTranslator.STRING;
        } else if (schema.type().equals(Schema.Type.INT64)) {
            // If not otherwise specified, long == unix time
            return TimeTranslator.UNIX;
        } else if (schema.type().equals(Schema.Type.FLOAT64)) {
            // If not otherwise specified, long == unix time
            return TimeTranslator.BIG_DECIMAL;
        }
        throw new ConnectException("Schema " + schema + " does not correspond to a known timestamp type format");
    }

    public static class Key<T extends ConnectRecord<T>> extends TimestampTransform<T>
            implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends TimestampTransform<T>
            implements KeyOrValueTransformation.Value<T> {
    }
}
