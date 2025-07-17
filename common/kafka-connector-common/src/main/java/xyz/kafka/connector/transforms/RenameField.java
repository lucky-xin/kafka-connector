package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import xyz.kafka.connector.validator.Validators;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/**
 * 修改字段名称
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class RenameField<R extends ConnectRecord<R>> extends AbstractTransformation<R> {

    public static final String RENAMES = "renames";

    private Map<String, String> pairs;

    protected RenameField() {
        super(new ConfigDef()
                .define(
                        RENAMES,
                        ConfigDef.Type.LIST,
                        NO_DEFAULT_VALUE,
                        Validators.nonEmptyList(),
                        ConfigDef.Importance.MEDIUM,
                        "Fields to convert."
                ));
    }

    @Override
    protected void configure(Map<String, ?> configs, AbstractConfig config) {
        pairs = config.getList(RENAMES).stream()
                .map(t -> t.split(":"))
                .collect(Collectors.toMap(
                        t -> t[0].strip(),
                        t -> t[1].strip()
                ));
    }

    @Override
    public R apply(R r) {
        Struct struct = Requirements.requireStructOrNull(value(r), r.topic());
        if (struct == null) {
            return r;
        }
        Schema schema = schema(r);
        Schema newSchema = null;
        if (isSchemaCache) {
            newSchema = schemaCache.get(r.topic(), k -> makeUpdatedSchema(schema));
        } else {
            newSchema = makeUpdatedSchema(schema);
        }
        Struct newValue = new Struct(newSchema);
        for (Field field : schema.fields()) {
            String newName = pairs.get(field.name());
            if (newName != null) {
                newValue.put(newName, struct.get(field));
                continue;
            }
            newValue.put(field.name(), struct.get(field));
        }
        return newRecord(r, newValue, newSchema);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            String newName = pairs.get(field.name());
            if (newName != null) {
                builder.field(newName, field.schema());
                continue;
            }
            builder.field(field.name(), field.schema());
        }
        return builder.build();
    }

    public static class Key<T extends ConnectRecord<T>> extends RenameField<T>
            implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends RenameField<T>
            implements KeyOrValueTransformation.Value<T> {
    }
}
