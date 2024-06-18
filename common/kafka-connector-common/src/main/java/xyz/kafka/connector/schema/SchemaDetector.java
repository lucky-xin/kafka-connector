package xyz.kafka.connector.schema;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * SchemaDetector
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@SuppressWarnings("unchecked")
public class SchemaDetector {
    private static final Logger log = LoggerFactory.getLogger(SchemaDetector.class);
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final long MILLIS_PER_DAY;
    protected static final Schema STATE_SCHEMA;
    public static final LogicalTypeMatcher DEFAULT_MATCHER;
    private static final Set<String> LOGICAL_TYPE_NAMES;
    protected final LogicalTypeMatcher logicalTypeMatcher;
    protected final FieldPath path;
    protected final boolean examineMapsAsStructs;
    protected final boolean examineNullFields;
    protected final SchemaValidator evolutionValidator;
    protected final SchemaValidator commonTypeValidator;
    protected final Function<FieldPath, String> nameFormatter;
    private final SchemaDefault defaults;
    private PathMatcher optionalFields;
    private SchemaValidator validator;
    protected String name;
    private Schema.Type knownType = null;
    private boolean optional = false;
    private boolean ignoredNullField = false;
    private Object defaultValue;
    private Integer version;
    private Map<String, String> parameters;
    private Map<String, SchemaDetector> fields;
    private SchemaDetector keyDetector;
    private SchemaDetector valueDetector;
    private Schema schema;
    private Struct saveState;

    @SuppressWarnings("unchecked")
    public static <B extends GenericBuilder<B, ? extends SchemaDetector>> B create() {
        return (B) new Builder();
    }

    protected SchemaDetector(GenericBuilder<?, ? extends SchemaDetector> builder) {
        this.defaultValue = SchemaDefault.NONE;
        this.path = builder.path;
        this.name = builder.name;
        this.evolutionValidator = builder.evolutionValidator();
        this.commonTypeValidator = builder.commonTypeValidator();
        this.nameFormatter = builder.nameFormatter();
        this.validator = this.evolutionValidator;
        this.examineMapsAsStructs = builder.examineMapsAsStructs;
        this.defaults = builder.defaultFunction();
        this.logicalTypeMatcher = builder.logicalTypeMatcher();
        this.optionalFields = builder.optionalFields();
        this.examineNullFields = builder.examineNullFields;
        if (this.optionalFields.matches(this.path)) {
            this.optional = true;
        }

    }

    protected SchemaDetector(SchemaDetector original) {
        this.defaultValue = SchemaDefault.NONE;
        this.path = original.path;
        this.name = original.name;
        this.evolutionValidator = original.evolutionValidator;
        this.commonTypeValidator = original.commonTypeValidator;
        this.nameFormatter = original.nameFormatter;
        this.validator = original.validator;
        this.examineMapsAsStructs = original.examineMapsAsStructs;
        this.logicalTypeMatcher = original.logicalTypeMatcher;
        this.optionalFields = original.optionalFields;
        this.optional = original.optional;
        this.ignoredNullField = original.ignoredNullField;
        this.defaults = original.defaults;
        this.knownType = original.knownType;
        this.version = original.version;
        this.parameters = original.parameters != null ? new LinkedHashMap<>(original.parameters) : null;
        this.fields = original.fields != null ? new LinkedHashMap<>(original.fields) : null;
        this.keyDetector = original.keyDetector != null ? original.keyDetector.clone() : null;
        this.valueDetector = original.valueDetector != null ? original.valueDetector.clone() : null;
        this.examineNullFields = original.examineNullFields;
    }

    protected SchemaDetector nestedDetector() {
        return create()
                .withLogicalTypeMatcher(this.logicalTypeMatcher)
                .withName(this.name)
                .withOptionalFields(this.optionalFields)
                .withDefaults(this.defaults)
                .withEvolutionValidator(SchemaValidators.noValidation())
                .withCommonTypeValidator(this.commonTypeValidator)
                .withNameFormatter(this.nameFormatter)
                .withPath(this.path)
                .withExamineNullFields(this.examineNullFields)
                .build();
    }

    protected SchemaDetector nestedDetector(String fieldName) {
        return create()
                .withLogicalTypeMatcher(this.logicalTypeMatcher)
                .withName(this.name)
                .withOptionalFields(this.optionalFields)
                .withDefaults(this.defaults)
                .withEvolutionValidator(SchemaValidators.noValidation())
                .withCommonTypeValidator(this.commonTypeValidator)
                .withNameFormatter(this.nameFormatter)
                .withPath(this.path)
                .withChildPath(fieldName)
                .withExamineNullFields(this.examineNullFields)
                .build();
    }

    @Override
    protected SchemaDetector clone() {
        return new SchemaDetector(this);
    }

    public SchemaDetector clear() {
        this.name = null;
        this.knownType = null;
        this.optional = false;
        this.ignoredNullField = false;
        this.version = null;
        this.parameters = null;
        this.fields = null;
        this.keyDetector = null;
        this.valueDetector = null;
        this.schema = null;
        this.saveState = null;
        return this;
    }

    public Schema schema() {
        if (this.schema != null) {
            return this.schema;
        } else if (this.knownType == null) {
            return null;
        } else {
            SchemaBuilder builder = this.builder();
            if (builder != null) {
                this.schema = builder.build();
            }

            return this.schema;
        }
    }

    public Struct save() {
        if (this.saveState == null) {
            Struct state = this.computeState();
            SchemaBuilder builder = SchemaBuilder.struct()
                    .field("state", state.schema());
            if (this.keyDetector != null) {
                builder.field("keyDetector", this.keyDetector.save().schema());
            }

            if (this.valueDetector != null) {
                builder.field("valueDetector", this.valueDetector.save().schema());
            }

            Struct struct = this.computeFields();
            if (struct != null) {
                builder.field("struct", struct.schema());
            }

            this.saveState = (new Struct(builder.build())).put("state", state);
            if (this.keyDetector != null) {
                this.saveState.put("keyDetector", this.keyDetector.save());
            }

            if (this.valueDetector != null) {
                this.saveState.put("valueDetector", this.valueDetector.save());
            }

            if (struct != null) {
                this.saveState.put("struct", struct);
            }

        }
        return this.saveState;
    }

    protected Struct computeState() {
        return (new Struct(STATE_SCHEMA))
                .put("validator", this.validator == this.evolutionValidator ? "evolution" : "common")
                .put("name", this.name)
                .put("knownType", this.knownType != null ? this.knownType.toString() : null)
                .put("optional", this.optional)
                .put("ignoredNullField", this.ignoredNullField)
                .put("version", this.version)
                .put("parameters", this.parameters);
    }

    protected Struct computeFields() {
        if (this.fields == null) {
            return null;
        } else {
            SchemaBuilder fieldsSchema = SchemaBuilder.struct();
            this.fields.forEach((k, v) -> fieldsSchema.field(k, v.save().schema()));
            Struct fieldsState = new Struct(fieldsSchema);
            this.fields.forEach((k, v) -> fieldsState.put(k, v.save()));
            return fieldsState;
        }
    }

    public void restore(Struct saveState) {
        if (saveState != null && saveState != this.saveState) {
            this.clear();
            Struct state = saveState.getStruct("state");
            this.validator = "evolution".equals(state.getString("validator")) ? this.evolutionValidator : this.commonTypeValidator;
            this.name = state.getString("name");
            this.knownType = state.getString("knownType") != null ? Schema.Type.valueOf(state.getString("knownType")) : null;
            this.optional = state.getBoolean("optional");
            this.ignoredNullField = state.getBoolean("ignoredNullField");
            this.version = state.getInt32("version");
            this.parameters = state.getMap("parameters");
            if (saveState.schema().field("keyDetector") != null) {
                this.keyDetector().restore(saveState.getStruct("keyDetector"));
            }

            if (saveState.schema().field("valueDetector") != null) {
                this.valueDetector().restore(saveState.getStruct("valueDetector"));
            }

            if (saveState.schema().field("fields") != null) {
                this.fields = new LinkedHashMap<>();
                Struct struct = saveState.getStruct("struct");
                for (Field f : struct.schema().fields()) {
                    SchemaDetector fieldDetector = this.nestedDetector(f.name());
                    fieldDetector.restore(struct.getStruct(f.name()));
                    this.fields.put(f.name(), fieldDetector);
                }
            }

            this.saveState = saveState;
        }
    }

    public SchemaBuilder builder() {
        if (this.knownType == null) {
            return null;
        } else {
            SchemaBuilder builder;
            if (this.knownType == Schema.Type.STRING) {
                builder = SchemaBuilder.type(this.knownType);
            } else {
                SchemaBuilder keyBuilder;
                if (this.knownType == Schema.Type.ARRAY) {
                    keyBuilder = this.valueDetector.builder();
                    if (keyBuilder == null) {
                        return null;
                    }

                    builder = SchemaBuilder.array(keyBuilder.build());
                } else if (this.knownType == Schema.Type.MAP) {
                    keyBuilder = this.keyDetector.builder();
                    SchemaBuilder valueBuilder = this.valueDetector.builder();
                    if (keyBuilder == null || valueBuilder == null) {
                        return null;
                    }

                    builder = SchemaBuilder.map(keyBuilder.build(), valueBuilder.build());
                } else {
                    builder = SchemaBuilder.type(this.knownType);
                }
            }

            if (this.knownType == Schema.Type.STRUCT && this.path != null) {
                builder.name(this.nameFormatter.apply(this.path));
            } else if (this.includeNameInSchema()) {
                builder.name(this.name);
            }

            if (this.optional) {
                builder.optional();
            }

            if (this.version != null) {
                builder.version(this.version);
            }

            if (this.knownType == Schema.Type.STRUCT && this.fields != null && !this.fields.isEmpty()) {
                this.fields.forEach((k, v) -> {
                    SchemaBuilder fieldBuilder = v.builder();
                    if (fieldBuilder == null) {
                        fieldBuilder = SchemaBuilder.type(Schema.Type.STRING).optional();
                    }

                    builder.field(k, fieldBuilder.schema());
                });
            }

            if (this.parameters != null && !this.parameters.isEmpty()) {
                this.parameters.forEach(builder::parameter);
            }

            if (builder.isOptional()) {
                if (this.defaultValue == SchemaDefault.NONE) {
                    this.defaultValue = this.defaults.computeDefault(this.path, builder);
                    log.trace("{}: Computed default value as {}", this, this.defaultValue);
                }

                if (this.defaultValue != SchemaDefault.NONE) {
                    builder.defaultValue(this.defaultValue);
                }
            }

            return builder;
        }
    }

    protected boolean includeNameInSchema() {
        if (this.name == null) {
            return false;
        } else if (this.knownType == Schema.Type.STRUCT) {
            return true;
        } else {
            return !this.knownType.isPrimitive() || LOGICAL_TYPE_NAMES.contains(this.name);
        }
    }

    public void examine(Object value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else if (value instanceof Number n) {
            this.examine(n);
        } else if (value instanceof Boolean b) {
            this.examine(b);
        } else if (value instanceof String s) {
            this.examine(s);
        } else if (value instanceof List<?> l) {
            this.examine(l);
        } else if (value instanceof Map<?, ?> m) {
            this.examine(m);
        } else if (value instanceof Struct s) {
            this.examine(s);
        } else if (value instanceof Date d) {
            this.examine(d);
        } else if (value instanceof byte[] b) {
            this.examine(b);
        } else if (value instanceof ByteBuffer b) {
            this.examine(b);
        } else {
            if (!(value instanceof Schema)) {
                throw new IllegalArgumentException("Unexpected value type: " + value.getClass());
            }

            this.examine((Schema) value);
        }

        this.validateChanges(previousSchema);
    }

    public void examine(Struct value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else {
            this.examine(value.schema());
        }

        this.validateChanges(previousSchema);
    }

    public void examine(Schema value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else if (this.setType(value.type())) {
            if (this.name == null) {
                this.name = value.name();
            }

            this.setDefaultValue(value.defaultValue());
            this.setOptional(this.optional || value.isOptional());
            this.version = value.version();
            switch (value.type()) {
                case MAP:
                    if (this.keyDetector == null) {
                        this.keyDetector = this.nestedDetector();
                    }

                    this.keyDetector.examine(this.schema.keySchema());
                case ARRAY:
                    if (this.valueDetector == null) {
                        this.valueDetector = this.nestedDetector();
                    }

                    this.valueDetector.examine(this.schema.valueSchema());
                    break;
                case STRUCT:
                    this.examineFields(value.fields(), Field::name, Field::schema);
                default:
                    this.keyDetector = null;
                    this.valueDetector = null;
                    break;
            }

            if (value.parameters() != null) {
                if (this.parameters == null) {
                    this.parameters = new HashMap<>();
                }

                this.parameters.putAll(value.parameters());
            }
        }

        this.validateChanges(previousSchema);
    }

    public void examine(Boolean value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else {
            this.setType(Schema.Type.BOOLEAN);
        }

        this.validateChanges(previousSchema);
    }

    public void examine(String value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else if (this.logicalTypeMatcher.isTimestampLiteral(value)) {
            this.setName(org.apache.kafka.connect.data.Timestamp.class.getName());
            this.setVersion(1);
            this.setType(Schema.Type.INT64);
        } else if (this.logicalTypeMatcher.isTimeLiteral(value)) {
            this.setName(org.apache.kafka.connect.data.Time.class.getName());
            this.setVersion(1);
            this.setType(Schema.Type.INT32);
        } else if (this.logicalTypeMatcher.isDateLiteral(value)) {
            this.setName(org.apache.kafka.connect.data.Date.class.getName());
            this.setVersion(1);
            this.setType(Schema.Type.INT32);
        } else {
            this.setType(Schema.Type.STRING);
        }

        this.validateChanges(previousSchema);
    }

    public void examine(Number value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else {
            if (value instanceof Byte) {
                this.setType(Schema.Type.INT8);
            } else if (value instanceof Short) {
                this.setType(Schema.Type.INT16);
            } else if (value instanceof Integer) {
                this.setType(Schema.Type.INT32);
            } else if (value instanceof Long) {
                this.setType(Schema.Type.INT64);
            } else if (value instanceof Float) {
                this.setType(Schema.Type.FLOAT32);
            } else if (value instanceof Double) {
                this.setType(Schema.Type.FLOAT64);
            } else if (value instanceof BigInteger) {
                this.setAsDecimal(0);
            } else if (value instanceof BigDecimal decimal) {
                this.setAsDecimal(decimal.scale());
            }

            this.validateChanges(previousSchema);
        }
    }

    public void examine(Map<?, ?> object) {
        log.trace("{}: Examining {}", this, object);
        Schema previousSchema = this.schema();
        if (object == null) {
            this.setOptional(true);
        } else {
            if (this.examineMapsAsStructs() && this.hasOnlyStringKeys(object)) {
                if (this.knownType == null) {
                    if (object.isEmpty()) {
                        this.valueDetector().setOptional(true);
                        this.optionalFields = PathMatcher.matchAll();
                    } else {
                        this.setType(Schema.Type.STRUCT);
                    }
                }
            } else if (this.knownType != Schema.Type.STRUCT) {
                this.setType(Schema.Type.MAP);
            } else {
                try {
                    this.valueDetector().beginCommonTypeDetection();
                    this.fields.values().forEach(det ->
                            this.valueDetector().examine(det.schema()));
                    this.fields.clear();
                    if (this.ignoredNullField) {
                        this.valueDetector().examine((Object) null);
                    }
                } finally {
                    this.valueDetector().endCommonTypeDetection();
                }

                this.setAsMap(Schema.STRING_SCHEMA, this.valueDetector().schema());
            }

            if (this.knownType == Schema.Type.MAP) {
                if (this.keyDetector == null && object.isEmpty()) {
                    this.knownType = null;
                    this.valueDetector().setOptional(true);
                    return;
                }

                if (this.keyDetector().knownType == Schema.Type.STRING
                        && this.valueDetector().knownType == Schema.Type.STRING) {
                    return;
                }

                boolean threwIllegalArgument = false;

                try {
                    this.keyDetector().beginCommonTypeDetection();
                    this.valueDetector().beginCommonTypeDetection();
                    Iterator<? extends Map.Entry<?, ?>> var4 = object.entrySet().iterator();
                    label303:
                    while (true) {
                        do {
                            if (!var4.hasNext()) {
                                break label303;
                            }
                            Map.Entry<?, ?> entry = var4.next();
                            try {
                                this.keyDetector().examine(entry.getKey());
                            } catch (IllegalArgumentException var17) {
                                threwIllegalArgument = true;
                            }

                            try {
                                this.valueDetector().examine(entry.getValue());
                            } catch (IllegalArgumentException var16) {
                                threwIllegalArgument = true;
                            }
                        } while (this.keyDetector().hasCachedSchema() && this.valueDetector().hasCachedSchema());
                        this.clearCachedSchema();
                    }
                } finally {
                    if (threwIllegalArgument) {
                        this.setAsMap(this.keyDetector().optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA,
                                this.valueDetector().optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                        this.clearCachedSchema();
                    }

                    this.keyDetector().endCommonTypeDetection();
                    this.valueDetector().endCommonTypeDetection();
                }
            } else if (this.knownType == Schema.Type.STRUCT) {
                this.examineFields(object.entrySet(), e -> e.getKey().toString(), Map.Entry::getValue);
            }
        }

        this.validateChanges(previousSchema);
    }

    public void examine(List<?> array) {
        log.trace("{}: Examining {}", this, array);
        Schema previousSchema = this.schema();
        if (array == null) {
            this.setOptional(true);
        } else if (this.setType(Schema.Type.ARRAY)) {
            this.examineArrayElements(array);
        }

        this.validateChanges(previousSchema);
    }

    public void examine(Date value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else {
            Calendar calendar = Calendar.getInstance(UTC);
            calendar.setTime(value);
            String newName;
            Schema.Type newType;
            if (this.isTime(calendar)) {
                newType = Schema.Type.INT32;
                newName = org.apache.kafka.connect.data.Time.class.getName();
            } else if (this.isDate(calendar)) {
                newType = Schema.Type.INT32;
                newName = org.apache.kafka.connect.data.Date.class.getName();
            } else {
                newType = Schema.Type.INT64;
                newName = org.apache.kafka.connect.data.Timestamp.class.getName();
            }

            if (this.setType(newType)) {
                this.setName(newName);
            }

            this.setVersion(1);
        }

        this.validateChanges(previousSchema);
    }

    public void examine(ByteBuffer value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else {
            this.setType(Schema.Type.BYTES);
        }

        this.validateChanges(previousSchema);
    }

    public void examine(byte[] value) {
        log.trace("{}: Examining {}", this, value);
        Schema previousSchema = this.schema();
        if (value == null) {
            this.setOptional(true);
        } else {
            this.setType(Schema.Type.BYTES);
        }

        this.validateChanges(previousSchema);
    }

    protected <FieldT> void examineFields(Iterable<FieldT> fields, Function<FieldT, String> nameAccessor, Function<FieldT, ?> valueAccessor) {
        this.examineFields(fields.iterator(), nameAccessor, valueAccessor);
    }

    protected <FieldT> void examineFields(Iterator<FieldT> fieldIter, Function<FieldT, String> nameAccessor, Function<FieldT, ?> valueAccessor) {
        if (this.knownType != Schema.Type.STRUCT) {
            throw new IllegalStateException("Found " + this.knownType + " schema type, but fields require " + Schema.Type.STRUCT);
        } else {
            Set<String> remainingFieldNames = null;
            if (this.fields == null) {
                this.fields = new LinkedHashMap<>();
            } else {
                remainingFieldNames = new HashSet<>(this.fields.keySet());
            }

            boolean allNewFields = this.fields.isEmpty() && !this.ignoredNullField;

            String fieldName;
            while (fieldIter.hasNext()) {
                FieldT field = fieldIter.next();
                fieldName = nameAccessor.apply(field);
                Object fieldValue = valueAccessor.apply(field);
                boolean newField = !this.fields.containsKey(fieldName);
                SchemaDetector fieldDetector = this.fields.containsKey(fieldName) ? this.fields.get(fieldName) : this.nestedDetector(fieldName);
                Schema previousFieldSchema = fieldDetector.schema();
                fieldDetector.examine(fieldValue);
                if (newField && !allNewFields) {
                    fieldDetector.setOptional(true);
                    this.clearCachedSchema();
                }

                if (fieldDetector.schema() != previousFieldSchema) {
                    this.clearCachedSchema();
                }

                if (remainingFieldNames != null) {
                    remainingFieldNames.remove(fieldName);
                }

                if (!this.examineNullFields() && fieldDetector.schema() == null && newField) {
                    this.ignoredNullField = true;
                } else {
                    this.fields.put(fieldName, fieldDetector);
                }
            }

            if (remainingFieldNames != null) {
                for (String remainingFieldName : remainingFieldNames) {
                    fieldName = remainingFieldName;
                    SchemaDetector fieldDetector = this.fields.get(fieldName);
                    fieldDetector.setOptional(true);
                    if (!fieldDetector.hasCachedSchema()) {
                        this.clearCachedSchema();
                    }
                }
            }

        }
    }

    protected void examineArrayElements(Iterable<?> elements) {
        this.examineArrayElements(elements.iterator());
    }

    protected void examineArrayElements(Iterator<?> elements) {
        if (this.knownType != Schema.Type.ARRAY) {
            throw new IllegalStateException("Found " + this.knownType + " schema type, but fields require " + Schema.Type.ARRAY);
        } else if (this.valueDetector == null && !elements.hasNext()) {
            this.knownType = null;
        } else {
            boolean threwIllegalArgument = false;

            try {
                this.valueDetector().beginCommonTypeDetection();

                while (elements.hasNext()) {
                    try {
                        this.valueDetector().examine(elements.next());
                    } catch (IllegalArgumentException var7) {
                        threwIllegalArgument = true;
                    }
                }

                if (!this.valueDetector().hasCachedSchema()) {
                    this.clearCachedSchema();
                }
            } finally {
                if (threwIllegalArgument) {
                    this.setAsArray(this.valueDetector().optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                    this.clearCachedSchema();
                }

                this.valueDetector().endCommonTypeDetection();
            }

        }
    }

    protected boolean hasOnlyStringKeys(Map<?, ?> object) {
        return object.entrySet().stream().allMatch((e) -> e.getKey() instanceof String);
    }

    protected PathMatcher optionalFields() {
        return this.optionalFields;
    }

    protected SchemaDetector setAsMap(Schema keyType, Schema valueType) {
        this.knownType = Schema.Type.MAP;
        this.keyDetector = this.nestedDetector();
        this.keyDetector.examine(keyType);
        this.valueDetector = this.nestedDetector();
        this.valueDetector.examine(valueType);
        this.keyDetector.clearCachedSchema();
        this.valueDetector.clearCachedSchema();
        this.clearCachedSchema();
        log.trace("{}: Set as map({}, {})", this, keyType, valueType);
        return this;
    }

    protected SchemaDetector setAsArray(Schema valueType) {
        this.knownType = Schema.Type.ARRAY;
        this.keyDetector = null;
        this.valueDetector = this.nestedDetector();
        this.valueDetector.examine(valueType);
        this.valueDetector.clearCachedSchema();
        this.clearCachedSchema();
        log.trace("{}: Set as array({})", this, valueType);
        return this;
    }

    protected SchemaDetector setAsDecimal(int scale) {
        if (this.setType(Schema.Type.BYTES)) {
            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
            this.setVersion(1);
            this.setDecimalScale(scale);
            log.trace("{}: Set as decimal({})", this, scale);
        }

        return this;
    }

    protected SchemaDetector setOptional(boolean optional) {
        if (this.optional != optional) {
            this.optional = optional;
            this.clearCachedSchema();
            log.trace("{}: Set optional to {}", this, this.optional);
        }

        return this;
    }

    protected SchemaDetector setDefaultValue(Object defaultValue) {
        Object newDefaultValue = defaultValue == null ? SchemaDefault.NONE : defaultValue;
        if (!Objects.equals(this.defaultValue, newDefaultValue)) {
            this.defaultValue = newDefaultValue;
            this.clearCachedSchema();
            log.trace("{}: Set default value to {}", this, this.defaultValue);
        }

        return this;
    }

    public SchemaDetector setName(String name) {
        if (!Objects.equals(this.name, name)) {
            this.name = name;
            this.clearCachedSchema();
            log.trace("{}: Set name to {}", this, this.name);
        }

        return this;
    }

    public void clearCachedSchema() {
        this.schema = null;
        this.saveState = null;
    }

    public boolean hasCachedSchema() {
        return this.schema != null;
    }

    protected boolean examineMapsAsStructs() {
        return this.examineMapsAsStructs;
    }

    protected boolean examineNullFields() {
        return this.examineNullFields;
    }

    protected boolean isTime(Calendar calendar) {
        long unixMillis = calendar.getTimeInMillis();
        return Math.abs(unixMillis) < MILLIS_PER_DAY;
    }

    protected boolean isDate(Calendar calendar) {
        return calendar.get(Calendar.HOUR_OF_DAY) == 0
                && calendar.get(Calendar.MINUTE) == 0
                && calendar.get(Calendar.SECOND) == 0
                && calendar.get(Calendar.MILLISECOND) == 0;
    }

    protected SchemaDetector keyDetector() {
        if (this.keyDetector == null) {
            this.keyDetector = this.nestedDetector();
        }

        return this.keyDetector;
    }

    protected SchemaDetector valueDetector() {
        if (this.valueDetector == null) {
            this.valueDetector = this.nestedDetector();
        }

        return this.valueDetector;
    }

    protected SchemaDetector fieldDetector(String fieldName) {
        return this.fields.get(fieldName);
    }

    protected SchemaDetector setVersion(Integer version) {
        if (!Objects.equals(this.version, version)) {
            this.version = version;
            this.clearCachedSchema();
            log.trace("{}: Set version {}", this, this.version);
        }

        return this;
    }

    protected SchemaDetector setDecimalScale(Integer scale) {
        if (scale == null) {
            this.setParameter("scale", null);
        } else {
            Integer existingScale = this.getDecimalScale();
            if (existingScale == null || scale.compareTo(existingScale) > 0) {
                this.setParameter("scale", scale.toString());
            }
        }

        return this;
    }

    protected Integer getDecimalScale() {
        if (this.parameters == null) {
            return null;
        } else {
            String value = this.parameters.get("scale");

            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException var3) {
                return null;
            }
        }
    }

    protected SchemaDetector setParameter(String name, String value) {
        if (value == null) {
            if (this.parameters != null && this.parameters.containsKey(name)) {
                Object oldValue = this.parameters.remove(name);
                this.clearCachedSchema();
                log.trace("{}: Remove parameter '{}'='{}'", this, name, oldValue);
            }
        } else {
            if (this.parameters == null) {
                this.parameters = new LinkedHashMap<>();
                log.trace("{}: Cleared parameters", this);
            }

            if (!this.parameters.containsKey(name) || !Objects.equals(this.parameters.get(name), value)) {
                this.parameters.put(name, value);
                this.clearCachedSchema();
                log.trace("{}: Set parameter '{}'='{}'", this, name, value);
            }
        }

        return this;
    }

    protected void unsetLogicalTypeName() {
        if (LOGICAL_TYPE_NAMES.contains(this.name)) {
            this.name = null;
        }

    }

    protected boolean setType(Schema.Type type) {
        try {
            this.knownType = this.determineType(this.knownType, type);
            this.clearCachedSchema();
            log.trace("{}: Set type to {}", this, this.knownType);
            return true;
        } catch (IllegalArgumentException var3) {
            this.knownType = Schema.Type.STRING;
            this.name = null;
            this.valueDetector = null;
            this.keyDetector = null;
            this.version = null;
            this.setDecimalScale((Integer) null);
            this.unsetLogicalTypeName();
            this.clearCachedSchema();
            return false;
        }
    }

    protected Schema.Type determineType(Schema.Type known, Schema.Type type) {
        if (type == null) {
            return known;
        } else if (known == null) {
            return type;
        } else {
            switch (known) {
                case MAP -> {
                    if (type == Schema.Type.MAP) {
                        return known;
                    }
                    throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                }
                case ARRAY -> {
                    if (type == Schema.Type.ARRAY) {
                        return known;
                    }
                    throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                }
                case STRUCT -> {
                    if (type == Schema.Type.STRUCT) {
                        return known;
                    }
                    throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                }
                case STRING -> {
                    if (type == Schema.Type.STRING) {
                        return known;
                    }
                    throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                }
                case INT8 -> {
                    switch (type) {
                        case INT8 -> {
                            return known;
                        }
                        case INT16, INT32, INT64, FLOAT32, FLOAT64 -> {
                            return type;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                        default ->
                                throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                    }
                }
                case INT16 -> {
                    switch (type) {
                        case INT8, INT16 -> {
                            return known;
                        }
                        case INT32, INT64, FLOAT32, FLOAT64 -> {
                            return type;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                        default ->
                                throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                    }
                }
                case INT32 -> {
                    switch (type) {
                        case INT8, INT16, INT32 -> {
                            return known;
                        }
                        case INT64, FLOAT32, FLOAT64 -> {
                            return type;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                        default ->
                                throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                    }
                }
                case INT64 -> {
                    switch (type) {
                        case INT8, INT16, INT32, INT64 -> {
                            return known;
                        }
                        case FLOAT32, FLOAT64 -> {
                            return type;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                        default ->
                                throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                    }
                }
                case FLOAT32 -> {
                    switch (type) {
                        case INT8, INT16, INT32 -> {
                            return known;
                        }
                        case INT64 -> {
                            return Schema.Type.FLOAT64;
                        }
                        case FLOAT32, FLOAT64 -> {
                            return type;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                        default ->
                                throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                    }
                }
                case FLOAT64 -> {
                    switch (type) {
                        case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64 -> {
                            return known;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                        default ->
                                throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
                    }
                }
                case BYTES -> {
                    if (org.apache.kafka.connect.data.Decimal.class.getName().equals(this.name)) {
                        switch (type) {
                            case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BYTES -> {
                                return known;
                            }
                        }
                    }
                }
                case BOOLEAN -> {
                    switch (type) {
                        case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN -> {
                            return type;
                        }
                        case BYTES -> {
                            this.setName(org.apache.kafka.connect.data.Decimal.class.getName());
                            return type;
                        }
                    }
                }
            }

            throw new IllegalArgumentException("Unable to find a common type for " + this.knownType + " and " + type);
        }
    }

    protected void beginCommonTypeDetection() {
        log.trace("{}: Begin common type detection", this);
        this.validator = this.commonTypeValidator;
    }

    protected void endCommonTypeDetection() {
        log.trace("{}: End common type detection", this);
        this.validator = this.evolutionValidator;
    }

    protected void validateChanges(Schema previousSchema) {
        if (previousSchema == null) {
            log.trace("{}: Validating {} (new)", this, previousSchema);
        } else if (this.schema == previousSchema) {
            log.trace("{}: Validating {} (unchanged)", this, this.schema);
        } else if (this.schema != null && this.schema.equals(previousSchema)) {
            log.trace("{}: Validating {} (equivalent)", this, this.schema);
        } else {
            Schema thisSchema = this.schema();
            log.trace("{}: Validating {} from {}", this, thisSchema, previousSchema);
            this.validator.validate(thisSchema, previousSchema);
            log.trace("{}: Validated {} from {}", this, thisSchema, previousSchema);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "/" + (this.path != null ? this.path.toString() : "");
    }

    static {
        MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);
        STATE_SCHEMA = SchemaBuilder.struct()
                .field("validator", Schema.STRING_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("knownType", Schema.OPTIONAL_STRING_SCHEMA)
                .field("optional", Schema.BOOLEAN_SCHEMA)
                .field("ignoredNullField", Schema.BOOLEAN_SCHEMA)
                .field("version", Schema.OPTIONAL_INT32_SCHEMA)
                .field("parameters", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
                .build();
        DEFAULT_MATCHER = new IsoLogicalTypeMatcher();
        LOGICAL_TYPE_NAMES = ImmutableSet.of(
                org.apache.kafka.connect.data.Decimal.class.getName(),
                "org.apache.kafka.connect.data.Timestamp",
                "org.apache.kafka.connect.data.Date",
                "org.apache.kafka.connect.data.Time");
    }

    public static class Builder extends GenericBuilder<Builder, SchemaDetector> {
        public Builder() {
        }
    }

    protected abstract static class GenericBuilder<SelfT, T extends SchemaDetector> {
        protected LogicalTypeMatcher logicalTypeMatcher;
        protected PathMatcher optionalFields;
        protected SchemaDefault defaultFunction;
        protected String name;
        protected FieldPath path;
        protected SchemaValidator evolutionValidator;
        protected SchemaValidator commonTypeValidator;
        protected Function<FieldPath, String> nameFormatter;
        protected boolean examineMapsAsStructs = true;
        protected boolean examineNullFields = true;

        protected GenericBuilder() {
        }

        public SelfT withLogicalTypeMatcher(LogicalTypeMatcher logicalTypeMatcher) {
            this.logicalTypeMatcher = logicalTypeMatcher;
            return this.self();
        }

        public SelfT withOptionalFields(PathMatcher optionalFields) {
            this.optionalFields = optionalFields;
            return this.self();
        }

        public SelfT withDefaults(SchemaDefault defaultFunction) {
            this.defaultFunction = defaultFunction;
            return this.self();
        }

        public SelfT withEvolutionValidator(SchemaValidator validator) {
            this.evolutionValidator = validator;
            return this.self();
        }

        public SelfT withCommonTypeValidator(SchemaValidator validator) {
            this.commonTypeValidator = validator;
            return this.self();
        }

        public SelfT withNameFormatter(Function<FieldPath, String> nameFormatter) {
            this.nameFormatter = nameFormatter;
            return this.self();
        }

        public SelfT withExamineMapsAsStructs(boolean considerFirstAsStructs) {
            this.examineMapsAsStructs = considerFirstAsStructs;
            return this.self();
        }

        public SelfT withName(String name) {
            this.name = name;
            return this.self();
        }

        public SelfT withPath(FieldPath path) {
            this.path = path;
            return this.self();
        }

        public SelfT withChildPath(String field) {
            if (field != null) {
                if (this.path != null) {
                    this.path = this.path.child(field);
                } else if (this.name != null) {
                    this.path = (new FieldPath(this.name)).child(field);
                } else {
                    this.path = new FieldPath(field);
                }
            }

            return this.self();
        }

        public SelfT withExamineNullFields(boolean examineNullFields) {
            this.examineNullFields = examineNullFields;
            return this.self();
        }

        public T build() {
            return (T) new SchemaDetector(this);
        }

        protected SelfT self() {
            return (SelfT) this;
        }

        protected SchemaValidator evolutionValidator() {
            return this.evolutionValidator != null ? this.evolutionValidator : this.defaultEvolutionValidator();
        }

        protected SchemaValidator defaultEvolutionValidator() {
            return SchemaValidators.noValidation();
        }

        protected SchemaValidator commonTypeValidator() {
            return this.commonTypeValidator != null ? this.commonTypeValidator : this.defaultCommonTypeValidator();
        }

        protected SchemaValidator defaultCommonTypeValidator() {
            return SchemaValidators.requireSameType().and(SchemaValidators.requireSameOptionality()).and(SchemaValidators.requireSameName());
        }

        protected Function<FieldPath, String> nameFormatter() {
            return this.nameFormatter != null ? this.nameFormatter : SchemaNameFormatters.avroCompatibleFormatter();
        }

        protected SchemaDefault defaultFunction() {
            return this.defaultFunction != null ? this.defaultFunction : SchemaDefaults.none();
        }

        protected LogicalTypeMatcher logicalTypeMatcher() {
            return this.logicalTypeMatcher != null ? this.logicalTypeMatcher : SchemaDetector.DEFAULT_MATCHER;
        }

        protected PathMatcher optionalFields() {
            return this.optionalFields != null ? this.optionalFields : PathMatcher.matchNone();
        }
    }
}
