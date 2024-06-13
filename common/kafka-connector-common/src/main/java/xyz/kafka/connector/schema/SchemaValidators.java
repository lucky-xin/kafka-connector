package xyz.kafka.connector.schema;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Objects;

/**
 * SchemaValidators
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class SchemaValidators {
    private static final SchemaValidator NO_VALIDATION = SchemaValidators::checkNothing;
    private static final SchemaValidator SAME_TYPE = SchemaValidators::checkSameType;
    private static final SchemaValidator SAME_NAME = SchemaValidators::checkSameName;
    private static final SchemaValidator SAME_OPTIONALITY = SchemaValidators::checkSameOptionality;
    private static final SchemaValidator EXPAND_NUMERICS = SchemaValidators::checkNumericExpansion;
    private static final SchemaValidator EXPAND_NUMERICS_WITH_DECIMAL = SchemaValidators::checkNumericExpansionWithDecimal;
    private static final SchemaValidator STRING_TO_BYTES = SchemaValidators::checkStringToBytes;
    private static final SchemaValidator SAME_TYPE_AND_OPTIONALITY;
    private static final SchemaValidator SAME_NAME_AND_TYPE_AND_OPTIONALITY;

    public static SchemaValidator noValidation() {
        return NO_VALIDATION;
    }

    public static SchemaValidator requireSameName() {
        return SAME_NAME;
    }

    public static SchemaValidator requireSameType() {
        return SAME_TYPE;
    }

    public static SchemaValidator allowNumericExpansion() {
        return EXPAND_NUMERICS;
    }

    public static SchemaValidator allowNumericExpansionWithDecimal() {
        return EXPAND_NUMERICS_WITH_DECIMAL;
    }

    public static SchemaValidator allowStringToBytes() {
        return STRING_TO_BYTES;
    }

    public static SchemaValidator requireSameOptionality() {
        return SAME_OPTIONALITY;
    }

    public static SchemaValidator requireSameTypeAndOptionality() {
        return SAME_TYPE_AND_OPTIONALITY;
    }

    public static SchemaValidator requireSameNameAndTypeAndOptionality() {
        return SAME_NAME_AND_TYPE_AND_OPTIONALITY;
    }

    static void failChangingType(Schema newSchema, Schema previous) {
        String msg;
        if (previous.name() == null && newSchema.name() == null) {
            msg = String.format("Cannot change type from %s to %s", previous.type(), newSchema.type());
        } else {
            msg = String.format("Cannot change type from %s (%s) to %s (%s)", previous.type(), previous.name(), newSchema.type(), newSchema.name());
        }

        throw new InvalidSchemaChangeException(previous, newSchema, msg);
    }

    static void checkSameOptionality(Schema newSchema, Schema previous) {
        if (previous.isOptional() != newSchema.isOptional()) {
            throw new InvalidSchemaChangeException(previous, newSchema, String.format("Cannot change optionality from %s (%s) to %s (%s)", previous.isOptional() ? "optional" : "required", previous.name(), newSchema.isOptional() ? "optional" : "required", newSchema.name()));
        }
    }

    static void checkSameName(Schema newSchema, Schema previous) {
        if (!Objects.equals(previous.name(), newSchema.name())) {
            failChangingType(newSchema, previous);
        }

    }

    static void checkNothing(Schema newSchema, Schema previous) {
    }

    static void checkSameType(Schema newSchema, Schema previous) {
        if (previous != newSchema) {
            if (previous.type() != newSchema.type()) {
                failChangingType(newSchema, previous);
            }

            checkSameName(newSchema, previous);
            if (isDecimal(previous) && isDecimal(newSchema)) {
                int previousScale = Integer.parseInt(previous.parameters().get("scale"));
                int newScale = Integer.parseInt(newSchema.parameters().get("scale"));
                if (newScale != previousScale) {
                    throw new InvalidSchemaChangeException(previous, newSchema, String.format("Cannot change decimal scale from %d to %d", previousScale, newScale));
                }
            }
        }
    }

    static void checkStringToBytes(Schema newSchema, Schema previous) {
        if (previous != newSchema
                && (previous.type() != newSchema.type() || !previous.type().isPrimitive() || !newSchema.type().isPrimitive())) {
            switch (previous.type()) {
                case STRING, BYTES:
                    switch (newSchema.type()) {
                        case STRING, BYTES -> {
                            return;
                        }
                    }
                default:
                    if (newSchema.type() != previous.type()) {
                        failChangingType(newSchema, previous);
                    }

                    if (Objects.equals(newSchema.name(), previous.name())) {
                        failChangingType(newSchema, previous);
                    }

                    return;
                case ARRAY:
                    if (newSchema.type() != Schema.Type.ARRAY) {
                        failChangingType(newSchema, previous);
                    }

                    checkStringToBytes(newSchema.valueSchema(), previous.valueSchema());
                    return;
                case MAP:
                    if (newSchema.type() != Schema.Type.MAP) {
                        failChangingType(newSchema, previous);
                    }

                    checkStringToBytes(newSchema.keySchema(), previous.keySchema());
                    checkStringToBytes(newSchema.valueSchema(), previous.valueSchema());
                    return;
                case STRUCT:
                    if (newSchema.type() != Schema.Type.STRUCT) {
                        failChangingType(newSchema, previous);
                    }

                    newSchema.fields().forEach((field) -> {
                        Field previousField = previous.field(field.name());
                        if (previousField != null) {
                            checkStringToBytes(field.schema(), previousField.schema());
                        }

                    });
            }

        }
    }

    static void checkNumericExpansionWithDecimal(Schema newSchema, Schema previous) {
        checkNumericExpansion(newSchema, previous, true);
    }

    static void checkNumericExpansion(Schema newSchema, Schema previous) {
        checkNumericExpansion(newSchema, previous, false);
    }

    static void checkNumericExpansion(Schema newSchema, Schema previous, boolean includeDecimal) {
        if (previous != newSchema && (previous.type() != newSchema.type() || !previous.type().isPrimitive() || !newSchema.type().isPrimitive())) {
            label112:
            switch (previous.type()) {
                case STRING -> {
                    if (Objects.requireNonNull(newSchema.type()) == Schema.Type.STRING) {
                        return;
                    }
                    failChangingType(newSchema, previous);
                }
                case BYTES -> {
                    if (isDecimal(previous) && isDecimal(newSchema) && includeDecimal) {
                        int previousScale = Integer.parseInt(previous.parameters().get("scale"));
                        int newScale = Integer.parseInt(newSchema.parameters().get("scale"));
                        if (newScale < previousScale) {
                            throw new InvalidSchemaChangeException(previous, newSchema, String.format("Cannot reduce decimal scale from %d to %d", previousScale, newScale));
                        }
                        return;
                    }
                }
                case ARRAY -> {
                    if (newSchema.type() != Schema.Type.ARRAY) {
                        failChangingType(newSchema, previous);
                    }
                    checkNumericExpansion(newSchema.valueSchema(), previous.valueSchema(), includeDecimal);
                    return;
                }
                case MAP -> {
                    if (newSchema.type() != Schema.Type.MAP) {
                        failChangingType(newSchema, previous);
                    }
                    checkNumericExpansion(newSchema.keySchema(), previous.keySchema(), includeDecimal);
                    checkNumericExpansion(newSchema.valueSchema(), previous.valueSchema(), includeDecimal);
                    return;
                }
                case STRUCT -> {
                    if (newSchema.type() != Schema.Type.STRUCT) {
                        failChangingType(newSchema, previous);
                    }
                    newSchema.fields().forEach(field -> {
                        Field previousField = previous.field(field.name());
                        if (previousField != null) {
                            checkNumericExpansion(field.schema(), previousField.schema(), includeDecimal);
                        }

                    });
                    return;
                }
                case BOOLEAN -> {
                    if (Objects.requireNonNull(newSchema.type()) == Schema.Type.BOOLEAN) {
                        return;
                    }
                    failChangingType(newSchema, previous);
                    break label112;
                }
                case INT8 -> {
                    switch (newSchema.type()) {
                        case BYTES:
                            if (includeDecimal && isDecimal(newSchema)) {
                                return;
                            }
                        case ARRAY, MAP, STRUCT, BOOLEAN:
                        default:
                            failChangingType(newSchema, previous);
                            break label112;
                        case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64:
                            return;
                    }
                }
                case INT16 -> {
                    switch (newSchema.type()) {
                        case BYTES:
                            if (includeDecimal && isDecimal(newSchema)) {
                                return;
                            }
                        case ARRAY, MAP, STRUCT, BOOLEAN, INT8:
                        default:
                            failChangingType(newSchema, previous);
                            break label112;
                        case INT16, INT32, INT64, FLOAT32, FLOAT64:
                            return;
                    }
                }
                case INT32 -> {
                    switch (newSchema.type()) {
                        case BYTES:
                            if (includeDecimal && isDecimal(newSchema)) {
                                return;
                            }
                        case ARRAY:
                        case MAP:
                        case STRUCT:
                        case BOOLEAN:
                        case INT8:
                        case INT16:
                        default:
                            failChangingType(newSchema, previous);
                            break label112;
                        case INT32:
                        case INT64:
                        case FLOAT32:
                        case FLOAT64:
                            return;
                    }
                }
                case INT64 -> {
                    switch (newSchema.type()) {
                        case BYTES:
                            if (includeDecimal && isDecimal(newSchema)) {
                                return;
                            }
                        default:
                            failChangingType(newSchema, previous);
                            break label112;
                        case INT64:
                        case FLOAT32:
                        case FLOAT64:
                            return;
                    }
                }
                case FLOAT32 -> {
                    switch (newSchema.type()) {
                        case BYTES:
                            if (includeDecimal && isDecimal(newSchema)) {
                                return;
                            }
                            break label112;
                        case FLOAT32:
                        case FLOAT64:
                            return;
                        default:
                            break label112;
                    }
                }
                case FLOAT64 -> {
                    switch (newSchema.type()) {
                        case BYTES -> {
                            if (includeDecimal && isDecimal(newSchema)) {
                                return;
                            }
                        }
                        case FLOAT64 -> {
                            return;
                        }
                    }
                }
            }

            if (newSchema.type() != previous.type()) {
                failChangingType(newSchema, previous);
            }

            if (Objects.equals(newSchema.name(), previous.name())) {
                failChangingType(newSchema, previous);
            }


        }
    }

    private static boolean isDecimal(Schema schema) {
        return schema.type() == Schema.Type.BYTES && "org.apache.kafka.connect.data.Decimal".equals(schema.name());
    }

    static {
        SAME_TYPE_AND_OPTIONALITY = SAME_TYPE.and(SAME_OPTIONALITY);
        SAME_NAME_AND_TYPE_AND_OPTIONALITY = SAME_TYPE_AND_OPTIONALITY.and(SAME_NAME);
    }
}
