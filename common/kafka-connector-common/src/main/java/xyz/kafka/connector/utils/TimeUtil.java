package xyz.kafka.connector.utils;

import java.time.Duration;

/**
 * TimeUtil
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class TimeUtil {
    public static String durationAsString(long millis) {
        return durationAsString(Duration.ofMillis(millis));
    }

    public static String durationAsString(Duration duration) {
        return String.format("%02d:%02d:%02d.%03d", duration.toHours(), duration.toMinutes() % 60, duration.getSeconds() % 60, (duration.getNano() / 1000000) % 1000);
    }
}
