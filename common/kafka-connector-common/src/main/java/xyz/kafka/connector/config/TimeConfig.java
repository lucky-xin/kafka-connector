package xyz.kafka.connector.config;

import cn.hutool.core.text.CharSequenceUtil;
import xyz.kafka.connector.enums.TimeTranslator;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.jetbrains.annotations.Nullable;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static xyz.kafka.connector.enums.TimeTranslator.UNIX;

/**
 * TimeConfig
 * <p>
 * This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
 * their behavior
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-09-25
 */
@Getter
public class TimeConfig {
    public static final String FIELDS = "fields";
    public static final String SOURCE_TYPE = "source.type";
    public static final String SOURCE_DATE_FORMAT = "source.date.format";
    public static final String SOURCE_TIME_ZONE = "source.time.zone";
    public static final String SOURCE_UNIX_PRECISION = "source.unix.precision";
    public static final String TARGET_TYPE = "target.type";
    public static final String TARGET_DATE_FORMAT = "target.date.format";
    public static final String TARGET_TIME_ZONE = "target.time.zone";
    public static final String TARGET_UNIX_PRECISION = "target.unix.precision";
    public static final String UNIX_PRECISION_MILLIS = "milliseconds";
    public static final String UNIX_PRECISION_MICROS = "microseconds";
    public static final String UNIX_PRECISION_NANOS = "nanoseconds";
    public static final String UNIX_PRECISION_SECONDS = "seconds";

    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static final Schema OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder()
            .optional().schema();
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
    public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();
    private final List<String> timeFields;
    private final TimeTranslator sourceType;
    private final SimpleDateFormat sourceFormat;
    private final TimeTranslator targetType;
    private final DateTimeFormatter targetFormat;
    private final TimeZone sourceZone;
    private final TimeZone targetZone;
    private final String sourceUnixPrecision;
    private final String targetUnixPrecision;

    public TimeConfig(AbstractConfig sc) {
        this.timeFields = Collections.unmodifiableList(sc.getList(FIELDS));
        this.sourceZone = TimeZone.getTimeZone(ZoneId.of(sc.getString(SOURCE_TIME_ZONE)));
        this.targetZone = TimeZone.getTimeZone(ZoneId.of(sc.getString(TARGET_TIME_ZONE)));
        this.sourceUnixPrecision = sc.getString(SOURCE_UNIX_PRECISION);
        this.targetUnixPrecision = sc.getString(TARGET_UNIX_PRECISION);
        this.sourceType = TimeTranslator.valueOf(sc.getString(SOURCE_TYPE));
        this.sourceFormat = getSimpleDateFormat(sourceType, sc.getString(SOURCE_DATE_FORMAT), sourceZone);
        this.targetType = TimeTranslator.valueOf(sc.getString(TARGET_TYPE));
        this.targetFormat = getDateTimeFormatter(targetType, sc.getString(TARGET_DATE_FORMAT), targetZone.toZoneId());
        validateUnixPrecision(sourceType, sourceUnixPrecision);
        validateUnixPrecision(targetType, targetUnixPrecision);
    }

    private void validateUnixPrecision(TimeTranslator tt, String tup) {
        if (UNIX == tt) {
            boolean label = UNIX_PRECISION_MILLIS.equals(tup)
                    || UNIX_PRECISION_MICROS.equals(tup)
                    || UNIX_PRECISION_NANOS.equals(tup)
                    || UNIX_PRECISION_SECONDS.equals(tup);
            if (!label) {
                throw new ConnectException("when type == UNIX, " +
                        "unix.precision must one of:milliseconds,microseconds,nanoseconds,seconds");
            }
        }
    }

    @Nullable
    private static SimpleDateFormat getSimpleDateFormat(TimeTranslator translator,
                                                        String format,
                                                        TimeZone timeZone) {
        if (TimeTranslator.STRING.equals(translator) && CharSequenceUtil.isEmpty(format)) {
            throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
        }
        SimpleDateFormat sdf = null;
        if (CharSequenceUtil.isNotEmpty(format)) {
            sdf = new SimpleDateFormat(format);
            sdf.setTimeZone(timeZone);
        }
        return sdf;
    }

    @Nullable
    private static DateTimeFormatter getDateTimeFormatter(TimeTranslator translator,
                                                          String format,
                                                          ZoneId zoneId) {
        if (TimeTranslator.STRING.equals(translator) && CharSequenceUtil.isEmpty(format)) {
            throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
        }
        DateTimeFormatter dtf = null;
        if (CharSequenceUtil.isNotEmpty(format)) {
            dtf = DateTimeFormatter.ofPattern(format).withZone(zoneId);
        }
        return dtf;
    }
}
