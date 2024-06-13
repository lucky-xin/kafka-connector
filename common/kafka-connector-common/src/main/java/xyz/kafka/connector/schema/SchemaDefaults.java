package xyz.kafka.connector.schema;

import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.Objects;

/**
 * SchemaDefaults
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class SchemaDefaults {
    private static final SchemaDefault ALWAYS_NONE = SchemaDefaults::alwaysNone;
    private static final SchemaDefault ALWAYS_NULL = SchemaDefaults::alwaysNull;
    private static final SchemaDefault EMPTY_STRING = SchemaDefaults::emptyString;
    private static final SchemaDefault NUMERIC_ZEROS = SchemaDefaults::numericZeros;
    private static final Map<Schema.Type, Object> NUMERIC_BY_TYPE;

    static {
        NUMERIC_BY_TYPE = Map.of(
                Schema.Type.INT8,
                (byte) 0, Schema.Type.INT16,
                (short) 0, Schema.Type.INT32,
                0, Schema.Type.INT64,
                0L, Schema.Type.FLOAT32,
                0.0f, Schema.Type.FLOAT64, 0.0d
        );
    }

    public static SchemaDefault none() {
        return ALWAYS_NONE;
    }

    static Object alwaysNone(FieldPath path, Schema schema) {
        return SchemaDefault.NONE;
    }

    public static SchemaDefault alwaysNull() {
        return ALWAYS_NULL;
    }

    static Object alwaysNull(FieldPath path, Schema schema) {
        return null;
    }

    public static SchemaDefault numericZeros() {
        return NUMERIC_ZEROS;
    }

    static Object numericZeros(FieldPath path, Schema schema) {
        return NUMERIC_BY_TYPE.get(schema.type());
    }

    public static SchemaDefault emptyString() {
        return EMPTY_STRING;
    }

    static Object emptyString(FieldPath path, Schema schema) {
        if (schema.type() == Schema.Type.STRING) {
            return "";
        }
        return null;
    }

    public static SchemaDefault literalDefault(Schema.Type expectedType, Object value) {
        Objects.requireNonNull(expectedType);
        return (path, schema) -> {
            if (schema.type() == expectedType) {
                return value;
            }
            return null;
        };
    }

    public static SchemaDefault literalDefault(FieldPath expectedPath, Object value) {
        Objects.requireNonNull(expectedPath);
        return literalDefault(PathMatcher.match(expectedPath), value);
    }

    public static SchemaDefault literalDefault(PathMatcher pathMatcher, Object value) {
        Objects.requireNonNull(pathMatcher);
        return (path, schema) -> {
            if (pathMatcher.matches(path != null ? path : FieldPath.EMPTY)) {
                return value;
            }
            return null;
        };
    }
}
