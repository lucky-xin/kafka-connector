package xyz.kafka.connector.schema;

import java.util.regex.Pattern;

/**
 * IsoLogicalTypeMatcher
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class IsoLogicalTypeMatcher implements LogicalTypeMatcher {
    private static final String ISO_TIMESTAMP_PATTERN_STR = "^(\\d{1,4})-(\\d{2})-(\\d{2})[Tt ](\\d{2}):(\\d{2}):(\\d{2})(\\.\\d{0,9})?(Z|([+-](\\d{2}):?(\\d{2})))?$";
    public static final Pattern ISO_TIMESTAMP_PATTERN = Pattern.compile(ISO_TIMESTAMP_PATTERN_STR);
    private static final String ISO_TIME_PATTERN_STR = "^(\\d{2}):(\\d{2}):(\\d{2})(\\.\\d{0,9})?(Z|([+-](\\d{2}):?(\\d{2})))?$";
    public static final Pattern ISO_TIME_PATTERN = Pattern.compile(ISO_TIME_PATTERN_STR);
    private static final String ISO_DATE_PATTERN_STR = "^(\\d{1,4})-(\\d{2})-(\\d{2})$";
    public static final Pattern ISO_DATE_PATTERN = Pattern.compile(ISO_DATE_PATTERN_STR);

    @Override
    public boolean isTimestampLiteral(String value) {
        return ISO_TIMESTAMP_PATTERN.matcher(value).matches();
    }

    @Override
    public boolean isTimeLiteral(String value) {
        return ISO_TIME_PATTERN.matcher(value).matches();
    }

    @Override
    public boolean isDateLiteral(String value) {
        return ISO_DATE_PATTERN.matcher(value).matches();
    }
}
