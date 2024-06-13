package xyz.kafka.utils;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * InstantUtil
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-03-31
 */
public class InstantUtil {
    private static final BigDecimal ONE_BILLION = new BigDecimal(1000000000L);

    public static Instant toInstant(BigDecimal value) {
        long seconds = value.longValue();
        int nanoseconds = extractNanosecondDecimal(value, seconds);
        return Instant.ofEpochSecond(seconds, nanoseconds);
    }

    public static int extractNanosecondDecimal(BigDecimal value, long integer) {
        return value.subtract(new BigDecimal(integer)).multiply(ONE_BILLION).intValue();
    }
}
