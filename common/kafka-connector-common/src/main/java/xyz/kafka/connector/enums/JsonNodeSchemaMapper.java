package xyz.kafka.connector.enums;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * JsonNode Schema匹配
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public enum JsonNodeSchemaMapper {
    /**
     *  16-bit signed integer
     *
     *  Note that if you have an unsigned 16-bit data source, {@link Schema.Type#INT32} will be required to safely capture all valid values
     */
    INT16(Schema.INT16_SCHEMA, JsonNode::isShort),

    /**
     * 32-bit signed integer
     * <p>
     * Note that if you have an unsigned 32-bit data source, {@link Schema.Type#INT64} will be required to safely capture all valid values
     */
    INT32(Schema.INT32_SCHEMA, JsonNode::isInt),
    /**
     * 64-bit signed integer
     * <p>
     * Note that if you have an unsigned 64-bit data source, the {@link Decimal} logical type (encoded as {@link Schema.Type#BYTES})
     * will be required to safely capture all valid values
     */
    INT64(Schema.INT64_SCHEMA, JsonNode::isLong),
    /**
     * 32-bit IEEE 754 floating point number
     */
    FLOAT32(Schema.FLOAT32_SCHEMA, JsonNode::isFloat),
    /**
     * 64-bit IEEE 754 floating point number
     */
    FLOAT64(Schema.FLOAT64_SCHEMA, JsonNode::isDouble),

    /**
     * Boolean value (true or false)
     */
    BOOLEAN(Schema.BOOLEAN_SCHEMA, JsonNode::isBoolean),
    /**
     * Character string that supports all Unicode characters.
     * <p>
     * Note that this does not imply any specific encoding (e.g. UTF-8) as this is an in-memory representation.
     */
    STRING(Schema.STRING_SCHEMA, JsonNode::isTextual),
    /**
     * Sequence of unsigned 8-bit bytes
     */
    BYTES(Schema.BYTES_SCHEMA, JsonNode::isBinary),
    /**
     * An ordered sequence of elements, each of which shares the same type.
     */
    ARRAY(null, JsonNode::isArray),
    /**
     * A mapping from keys to values. Both keys and values can be arbitrarily complex types, including complex types
     * such as {@link Struct}.
     */
    MAP(null, JsonNode::isObject);
    private final Schema schema;

    private final Predicate<JsonNode> test;

    JsonNodeSchemaMapper(Schema schema, Predicate<JsonNode> test) {
        this.schema = schema;
        this.test = test;
    }

    public Schema getSchema() {
        return schema;
    }

    public Predicate<JsonNode> getTest() {
        return test;
    }

    public static Optional<JsonNodeSchemaMapper> from(JsonNode node) {
        if (node == null) {
            return Optional.empty();
        }
        return Stream.of(values())
                .filter(t -> t.test.test(node))
                .findFirst();
    }
}
