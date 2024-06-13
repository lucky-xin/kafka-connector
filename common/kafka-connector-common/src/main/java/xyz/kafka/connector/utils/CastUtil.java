package xyz.kafka.connector.utils;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Set;

/**
 * Cast Util
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2023/10/9
 */
public class CastUtil {

    private enum FieldType {
        INPUT, OUTPUT
    }

    private static final Set<Schema.Type> SUPPORTED_CAST_INPUT_TYPES = EnumSet.of(
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
            Schema.Type.FLOAT32, Schema.Type.FLOAT64, Schema.Type.BOOLEAN,
            Schema.Type.STRING, Schema.Type.BYTES
    );

    private static final Set<Schema.Type> SUPPORTED_CAST_OUTPUT_TYPES = EnumSet.of(
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
            Schema.Type.FLOAT32, Schema.Type.FLOAT64, Schema.Type.BOOLEAN,
            Schema.Type.STRING
    );

    public static Object castValueToType(Schema schema, Object value, Schema.Type targetType) {
        try {
            if (value == null) {
                return null;
            }
            Schema.Type inferredType = schema == null ? ConnectSchema.schemaType(value.getClass()) :
                    schema.type();
            if (inferredType == null) {
                throw new DataException("Cast transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }
            // Ensure the type we are trying to cast from is supported
            validCastType(inferredType, FieldType.INPUT);

            // Perform logical type encoding to their internal representation.
            if (schema != null && schema.name() != null && targetType != Schema.Type.STRING) {
                value = encodeLogicalType(schema, value);
            }

            return switch (targetType) {
                case INT8 -> castToInt8(value);
                case INT16 -> castToInt16(value);
                case INT32 -> castToInt32(value);
                case INT64 -> castToInt64(value);
                case FLOAT32 -> castToFloat32(value);
                case FLOAT64 -> castToFloat64(value);
                case BOOLEAN -> castToBoolean(value);
                case STRING -> castToString(value);
                default -> throw new DataException(targetType + " is not supported in the Cast transformation.");
            };
        } catch (NumberFormatException e) {
            throw new DataException("Value (" + value.toString() + ") was out of range for requested data type", e);
        }
    }

    private static Object encodeLogicalType(Schema schema, Object value) {
        return switch (schema.name()) {
            case Date.LOGICAL_NAME -> Date.fromLogical(schema, (java.util.Date) value);
            case Time.LOGICAL_NAME -> Time.fromLogical(schema, (java.util.Date) value);
            case Timestamp.LOGICAL_NAME -> Timestamp.fromLogical(schema, (java.util.Date) value);
            default -> value;
        };
    }

    private static byte castToInt8(Object value) {
        if (value instanceof Number n)
            return n.byteValue();
        else if (value instanceof Boolean b)
            return Boolean.TRUE.equals(b) ? (byte) 1 : (byte) 0;
        else if (value instanceof String s)
            return Byte.parseByte(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static short castToInt16(Object value) {
        if (value instanceof Number n)
            return n.shortValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (short) 1 : (short) 0;
        else if (value instanceof String s)
            return Short.parseShort(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static int castToInt32(Object value) {
        if (value instanceof Number n)
            return n.intValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1 : 0;
        else if (value instanceof String s)
            return Integer.parseInt(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static long castToInt64(Object value) {
        if (value instanceof Number n)
            return n.longValue();
        else if (value instanceof Boolean b)
            return Boolean.TRUE.equals(b) ? (long) 1 : (long) 0;
        else if (value instanceof String s)
            return Long.parseLong(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static float castToFloat32(Object value) {
        if (value instanceof Number n)
            return n.floatValue();
        else if (value instanceof Boolean b)
            return Boolean.TRUE.equals(b) ? 1.f : 0.f;
        else if (value instanceof String s)
            return Float.parseFloat(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static double castToFloat64(Object value) {
        if (value instanceof Number n)
            return n.doubleValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1. : 0.;
        else if (value instanceof String s)
            return Double.parseDouble(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static boolean castToBoolean(Object value) {
        if (value instanceof Number n)
            return n.longValue() != 0L;
        else if (value instanceof Boolean b)
            return b;
        else if (value instanceof String s)
            return Boolean.parseBoolean(s);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static String castToString(Object value) {
        if (value instanceof java.util.Date d) {
            return Values.dateFormatFor(d).format(d);
        } else if (value instanceof ByteBuffer bf) {
            return Base64.getEncoder().encodeToString(Utils.readBytes(bf));
        } else if (value instanceof byte[] rawBytes) {
            return Base64.getEncoder().encodeToString(rawBytes);
        } else {
            return value.toString();
        }
    }

    private static Schema.Type validCastType(Schema.Type type, FieldType fieldType) {
        if (FieldType.INPUT == fieldType && !SUPPORTED_CAST_INPUT_TYPES.contains(type)) {
            throw new DataException("Cast transformation does not support casting from " +
                    type + "; supported types are " + SUPPORTED_CAST_INPUT_TYPES);
        } else if (FieldType.OUTPUT == fieldType && !SUPPORTED_CAST_OUTPUT_TYPES.contains(type)) {
            throw new ConfigException("Cast transformation does not support casting to " +
                    type + "; supported types are " + SUPPORTED_CAST_OUTPUT_TYPES);
        }
        return type;
    }
}
