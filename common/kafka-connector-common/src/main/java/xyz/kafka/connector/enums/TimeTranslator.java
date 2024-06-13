package xyz.kafka.connector.enums;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import xyz.kafka.connector.config.TimeConfig;
import xyz.kafka.utils.InstantUtil;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * TimeType
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-09-25
 */
public enum TimeTranslator {

    STRING {
        @Override
        public Instant toInstant(TimeConfig config, Object orig) {
            if (!(orig instanceof String text)) {
                throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
            }
            try {
                return config.getSourceFormat().parse(text).toInstant();
            } catch (ParseException e) {
                throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                        + config.getSourceFormat().toPattern() + ")", e);
            }
        }

        @Override
        public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
        }

        @Override
        public Object toTarget(TimeConfig config, Instant instant) {
            return config.getTargetFormat().format(instant);
        }
    },
    UNIX {
        @Override
        public Instant toInstant(TimeConfig config, Object orig) {
            if (!(orig instanceof Long unixTime)) {
                throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
            }
            return switch (config.getSourceUnixPrecision()) {
                case TimeConfig.UNIX_PRECISION_SECONDS ->
                        Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.SECONDS.toMillis(unixTime)).toInstant();
                case TimeConfig.UNIX_PRECISION_MICROS ->
                        Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.MICROSECONDS.toMillis(unixTime)).toInstant();
                case TimeConfig.UNIX_PRECISION_NANOS ->
                        Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.NANOSECONDS.toMillis(unixTime)).toInstant();
                default -> Timestamp.toLogical(Timestamp.SCHEMA, unixTime).toInstant();
            };
        }

        @Override
        public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
        }

        @Override
        public Object toTarget(TimeConfig config, Instant instant) {
            long epochMilli = instant.toEpochMilli();
            return switch (config.getTargetUnixPrecision()) {
                case TimeConfig.UNIX_PRECISION_SECONDS -> TimeUnit.MILLISECONDS.toSeconds(epochMilli);
                case TimeConfig.UNIX_PRECISION_MICROS -> TimeUnit.MILLISECONDS.toMicros(epochMilli);
                case TimeConfig.UNIX_PRECISION_NANOS -> TimeUnit.MILLISECONDS.toNanos(epochMilli);
                default -> epochMilli;
            };
        }
    },
    DATE {
        @Override
        public Instant toInstant(TimeConfig config, Object orig) {
            if (!(orig instanceof Date d)) {
                throw new DataException("Expected Date to be a java.util.Date, but found " + orig.getClass());
            }
            // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
            return d.toInstant();
        }

        @Override
        public Schema typeSchema(boolean isOptional) {
            return isOptional ? TimeConfig.OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
        }

        @Override
        public Object toTarget(TimeConfig config, Instant instant) {
            Calendar result = Calendar.getInstance(TimeConfig.UTC);
            result.setTime(Date.from(instant));
            result.set(Calendar.HOUR_OF_DAY, 0);
            result.set(Calendar.MINUTE, 0);
            result.set(Calendar.SECOND, 0);
            result.set(Calendar.MILLISECOND, 0);
            return result.getTime().toInstant();
        }
    },
    TIME {
        @Override
        public Instant toInstant(TimeConfig config, Object orig) {
            if (!(orig instanceof Date d)) {
                throw new DataException("Expected Time to be a java.util.Date, but found " + orig.getClass());
            }
            // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
            return d.toInstant();
        }

        @Override
        public Schema typeSchema(boolean isOptional) {
            return isOptional ? TimeConfig.OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
        }

        @Override
        public Instant toTarget(TimeConfig config, Instant instant) {
            Calendar origCalendar = Calendar.getInstance(TimeConfig.UTC);
            origCalendar.setTime(Date.from(instant));
            Calendar result = Calendar.getInstance(config.getTargetZone());
            result.setTimeInMillis(0L);
            result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
            result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
            result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
            result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
            return result.getTime().toInstant();
        }
    },
    TIMESTAMP {
        @Override
        public Instant toInstant(TimeConfig config, Object orig) {
            if (!(orig instanceof Date d)) {
                throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
            }
            return d.toInstant();
        }

        @Override
        public Schema typeSchema(boolean isOptional) {
            return isOptional ? TimeConfig.OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
        }

        @Override
        public Object toTarget(TimeConfig config, Instant instant) {
            return instant.toEpochMilli();
        }
    },
    BIG_DECIMAL {
        @Override
        public Instant toInstant(TimeConfig config, Object orig) {
            if (!(orig instanceof Double d)) {
                throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
            }
            return InstantUtil.toInstant(BigDecimal.valueOf(d));
        }

        @Override
        public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
        }

        @Override
        public Object toTarget(TimeConfig config, Instant instant) {
            return new BigDecimal(toDecimal(instant.getEpochSecond(), instant.getNano()))
                    .doubleValue();
        }
    };

    private static final char[] ZEROES = new char[]{'0', '0', '0', '0', '0', '0', '0', '0', '0'};

    public static String toDecimal(long seconds, int nanoseconds) {
        StringBuilder string = new StringBuilder(Integer.toString(nanoseconds));
        if (string.length() < 9)
            string.insert(0, ZEROES, 0, 9 - string.length());
        return seconds + "." + string;
    }

    /**
     * Convert from the type-specific format to the universal Instant format
     */
    public abstract Instant toInstant(TimeConfig config, Object orig);

    /**
     * Get the schema for this format.
     */
    public abstract Schema typeSchema(boolean isOptional);

    /**
     * Convert from the universal instant format to the type-specific format
     */
    public abstract Object toTarget(TimeConfig config, Instant instant);
}
