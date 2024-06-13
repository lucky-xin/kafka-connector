package xyz.kafka.connector.utils;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Strings
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class Strings {


    public static <E> String readableJoin(CharSequence delim, Collection<E> items) {
        return "either " + items.stream().map(o -> {
            if (o != null) {
                return o.toString();
            }
            return null;
        }).collect(Collectors.joining(", or "));
    }

    public static int firstIndexOf(String str, char c1, char c2, int startIndex) {
        int c1Index = str.indexOf(c1, startIndex);
        int c2Index = str.indexOf(c2, startIndex);
        if (c1Index == -1) {
            return c2Index;
        }
        if (c2Index == -1) {
            return c1Index;
        }
        return Math.min(c1Index, c2Index);
    }

    public static String removeLeadingWhitespace(String input) {
        int len = Objects.requireNonNull(input).length();
        if (len == 0) {
            return input;
        }
        int index = 0;
        char[] val = input.toCharArray();
        while (index < len && val[index] <= ' ') {
            index++;
        }
        return index > 0 ? input.substring(index, len) : input;
    }

    public static String removeTrailingWhitespace(String input) {
        int len = Objects.requireNonNull(input).length();
        if (len == 0) {
            return input;
        }
        char[] val = input.toCharArray();
        while (len > 0 && val[len - 1] <= ' ') {
            len--;
        }
        return len < input.length() ? input.substring(0, len) : input;
    }
}
