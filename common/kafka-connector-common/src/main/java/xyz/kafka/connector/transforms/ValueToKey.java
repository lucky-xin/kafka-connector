package xyz.kafka.connector.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import xyz.kafka.connector.validator.Validators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Transformation that converts a complex value into a JSON string.
 * Can be used as a workaround for <a href="https://github.com/confluentinc/kafka-connect-jdbc/issues/46">...</a>
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class ValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_CONFIG = "fields";

    public static final String ADD_MODEL_CONFIG = "model";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    FIELDS_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    "Field names on the record value to extract as the record key."
            ).define(
                    ADD_MODEL_CONFIG,
                    ConfigDef.Type.STRING,
                    AddModel.APPEND.name(),
                    Validators.oneOfUppercase(AddModel.class),
                    ConfigDef.Importance.HIGH,
                    "Add value to key pattern."
            );

    private static final String PURPOSE = "copying fields from value to key";

    private List<String> fields;
    private AddModel addModel;

    private Cache<Schema, Schema> valueToKeySchemaCache;

    public enum AddModel {
        /**
         * 覆盖
         */
        OVERRIDE,
        /**
         * 追加
         */
        APPEND;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
        addModel = AddModel.valueOf(config.getString(ADD_MODEL_CONFIG));
        valueToKeySchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R r) {
        if (r.valueSchema() == null) {
            return applySchemaless(r);
        } else {
            return applyWithSchema(r);
        }
    }

    private R applySchemaless(R r) {
        final Map<String, Object> value = requireMap(r.value(), PURPOSE);
        final Map<String, Object> key = new HashMap<>(fields.size());
        for (String field : fields) {
            key.put(field, value.get(field));
        }
        return r.newRecord(r.topic(), r.kafkaPartition(), null, key, r.valueSchema(), r.value(), r.timestamp());
    }

    private R applyWithSchema(R r) {
        Object keyVal = r.key();
        Struct key = null;
        Schema keySchema = r.keySchema();
        if (keyVal != null) {
            key = requireStruct(keyVal, PURPOSE);
        }

        Struct value = requireStruct(r.value(), PURPOSE);
        Schema valueSchema = r.valueSchema();
        Schema newKeySchema = valueToKeySchemaCache.get(value.schema());
        boolean label = false;
        if (newKeySchema == null) {
            SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
            for (String field : fields) {
                Field fieldFromValue = valueSchema.field(field);
                if (fieldFromValue == null) {
                    throw new DataException("Field does not exist: " + field);
                }
                keySchemaBuilder.field(field, fieldFromValue.schema());
            }
            label = keySchema != null
                    && Schema.Type.STRUCT == keySchema.type()
                    && addModel == AddModel.APPEND;
            if (label) {
                for (Field field : keySchema.fields()) {
                    keySchemaBuilder.field(field.name(), field.schema());
                }
            }
            newKeySchema = keySchemaBuilder.build();
            valueToKeySchemaCache.put(valueSchema, newKeySchema);
        }

        Struct newKey = new Struct(newKeySchema);
        for (String field : fields) {
            newKey.put(field, value.get(field));
        }
        label = keySchema != null
                && Schema.Type.STRUCT == keySchema.type()
                && key != null
                && addModel == AddModel.APPEND;
        if (label) {
            for (Field field : keySchema.fields()) {
                newKey.put(field, key.get(field));
            }
        }
        return r.newRecord(r.topic(), r.kafkaPartition(), newKeySchema, newKey, value.schema(), value, r.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        valueToKeySchemaCache = null;
    }

}
