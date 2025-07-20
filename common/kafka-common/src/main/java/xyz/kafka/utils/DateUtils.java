package xyz.kafka.utils;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.TimeZone;

/**
 * DateUtils
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2025-07-18
 */
public class DateUtils {

    /**
     * 转换Date对象的时区表示（不改变实际时间点）
     *
     * @param date 要转换的Date对象
     * @param dst  目标时区
     * @return 调整时区后的新Date对象
     */
    public static Date convertTo(Date date, TimeZone dst) {
        ZonedDateTime zdt = Instant.ofEpochMilli(date.getTime()).atZone(dst.toZoneId());
        Instant instant = zdt.toInstant();
        return Date.from(instant);
    }
}
