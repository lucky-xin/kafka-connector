package xyz.kafka.connector.schema;

import org.apache.kafka.connect.errors.DataException;

import java.util.function.Function;

/**
 * SchemaNameFormatters
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class SchemaNameFormatters {
    public static Function<FieldPath, String> defaultFormatter() {
        return avroCompatibleFormatter();
    }

    public static Function<FieldPath, String> jsonPathFormatter() {
        return fieldPath -> {
            if (fieldPath == null) {
                return null;
            }
            return fieldPath.asString();
        };
    }

    public static Function<FieldPath, String> avroCompatibleFormatter() {
        return new AvroCompatibleFormatter();
    }

    public static class AvroCompatibleFormatter implements Function<FieldPath, String> {
        private AvroCompatibleFormatter() {
        }

        @Override
        public String apply(FieldPath fieldPath) {
            if (fieldPath == null) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            fieldPath.onPath(node -> {
                if (!sb.isEmpty()) {
                    sb.append('.');
                }
                if (node.isArrayIndex()) {
                    sb.append(node.index());
                    return;
                }
                char[] charArray = node.name().toCharArray();
                for (char c : charArray) {
                    if (Character.isLetterOrDigit(c) || c == '_') {
                        sb.append(c);
                    } else {
                        sb.append('_');
                    }
                }
            });
            String name = sb.toString();
            if (name.isEmpty()) {
                throw new DataException("Empty name for struct schema is invalid.");
            }
            char c = name.charAt(0);
            if (Character.isLetter(c) || c == '_') {
                return name;
            }
            return "_" + name;
        }
    }
}
