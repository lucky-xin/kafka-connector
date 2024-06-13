package xyz.kafka.serialization.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Getter;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.time.ZoneOffset.UTC;

/**
 * LogicalTypeConverter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-20
 */
@Getter
public class LogicalTypeConverterHolder {
    private static final BigDecimal ONE_BILLION = new BigDecimal(1000000000L);

    private static final Map<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

    private static final JsonNodeFactory JSON_NODE_FACTORY = new JsonNodeFactory(true);

    static {
        LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof BigDecimal decimal)) {
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                }
                return switch (config.decimalFormat()) {
                    case NUMERIC -> JSON_NODE_FACTORY.numberNode(decimal);
                    case BASE64 -> JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
                    default -> throw new DataException("Unexpected " + JsonConverterConfig.DECIMAL_FORMAT_CONFIG +
                            ": " + config.decimalFormat());
                };
            }

            @Override
            public Object toConnectData(final Schema schema, final JsonNode value) {
                if (value.isBigDecimal()) {
                    return value.decimalValue();
                }
                if (value.isBinary() || value.isTextual()) {
                    try {
                        return Decimal.toLogical(schema, value.binaryValue());
                    } catch (Exception e) {
                        throw new DataException("Invalid bytes for Decimal field", e);
                    }
                }

                throw new DataException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value.getNodeType());
            }
        });

        LOGICAL_CONVERTERS.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof java.util.Date date)) {
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                }
                return JSON_NODE_FACTORY.numberNode(org.apache.kafka.connect.data.Date.fromLogical(schema, date));
            }

            @Override
            public Object toConnectData(final Schema schema, final JsonNode value) {
                if (value instanceof TextNode tn) {
                    LocalDate ld = LocalDate.parse(tn.textValue(), DateTimeFormatter.ISO_DATE);
                    return org.apache.kafka.connect.data.Date.toLogical(schema, (int) ld.toEpochDay());
                }
                if (!(value.isIntegralNumber())) {
                    throw new DataException("Invalid type for Date, underlying representation should be integer but was " + value.getNodeType());
                }
                return org.apache.kafka.connect.data.Date.toLogical(schema, value.intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof java.util.Date date)) {
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                }
                return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, date));
            }

            @Override
            public Object toConnectData(final Schema schema, final JsonNode value) {
                if (value instanceof TextNode tn) {
                    LocalTime lt = LocalTime.parse(tn.textValue(), DateTimeFormatter.ISO_TIME);
                    return lt.atOffset(ZoneOffset.UTC)
                            .atDate(LocalDate.EPOCH)
                            .toInstant().toEpochMilli();
                }
                if (!(value.isIntegralNumber())) {
                    throw new DataException("Invalid type for Time, underlying representation should be integer but was " + value.getNodeType());
                }
                return Time.toLogical(schema, value.intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJsonData(final Schema schema, final Object value, final JsonDataConfig config) {
                if (!(value instanceof java.util.Date date)) {
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                }
                return JSON_NODE_FACTORY.numberNode(Timestamp.fromLogical(schema, date));
            }

            @Override
            public Object toConnectData(final Schema schema, final JsonNode value) {
                if (value instanceof TextNode tn) {
                    long millis = LocalDateTime.parse(tn.textValue(), DateTimeFormatter.ISO_DATE_TIME)
                            .atZone(UTC)
                            .toInstant()
                            .toEpochMilli();
                    return Timestamp.toLogical(schema, millis);
                }
                if (value.isIntegralNumber()) {
                    return Timestamp.toLogical(schema, value.longValue());
                }
                if (!(value.isNumber())) {
                    throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getNodeType());
                }
                Instant instant = toInstant(value instanceof DecimalNode dn
                        ? dn.decimalValue()
                        : BigDecimal.valueOf(value.doubleValue())
                );
                return Date.from(instant);
            }
        });
    }
    public static Instant toInstant(BigDecimal value) {
        long seconds = value.longValue();
        int nanoseconds = extractNanosecondDecimal(value, seconds);
        return Instant.ofEpochSecond(seconds, nanoseconds);
    }

    public static int extractNanosecondDecimal(BigDecimal value, long integer) {
        return value.subtract(new BigDecimal(integer)).multiply(ONE_BILLION).intValue();
    }

    public static Map<String, LogicalTypeConverter> getLogicalConverters() {
        return Collections.unmodifiableMap(LOGICAL_CONVERTERS);
    }
}