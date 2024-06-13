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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * FilterFields
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class Filter<T extends ConnectRecord<T>> extends AbstractTransformation<T> implements KeyOrValueTransformation<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Filter.class);

    private static final String CONFIG_FIELD = "field";
    private static final String CONFIG_DENY_LIST = "deny_list";
    private static final String CONFIG_ALLOW_LIST = "allow_list";

    private List<String> denylist;
    private List<String> allowlist;
    private String fieldName;

    protected Filter() {
        super(new ConfigDef()
                .define(CONFIG_FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Name of the field whose keys will be filtered")
                .define(CONFIG_DENY_LIST, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH,
                        "Keys that will be dropped from the field")
                .define(CONFIG_ALLOW_LIST, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH,
                        "Keys that will be kept in the field")
        );
    }

    @Override
    public void configure(Map<String, ?> configs, AbstractConfig config) {
        this.fieldName = config.getString(CONFIG_FIELD);
        this.denylist = config.getList(CONFIG_DENY_LIST);
        this.allowlist = config.getList(CONFIG_ALLOW_LIST);
    }

    @Override
    public T apply(T t) {
        Struct struct = Requirements.requireStructOrNull(value(t), t.topic());
        if (struct == null) {
            return null;
        }
        Schema schema = schema(t);
        Object val = struct.get(fieldName);
        if (val == null) {
            LOG.info("Field {} not found. Skipping filter.", fieldName);
            return t;
        }

        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        List<Field> fields = schema.fields();
        for (Field field : fields) {
            if (!denylist.isEmpty() && denylist.contains(field.name()) && allowlist.isEmpty()) {
                builder.field(field.name(), field.schema());
                continue;
            }
            if (allowlist.contains(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }
        Schema newSchema = builder.build();
        Struct s = new Struct(newSchema);
        for (Field field : newSchema.fields()) {
            s.put(field, struct.get(field));
        }

        return newRecord(t, s, newSchema);
    }

    public static class Key<T extends ConnectRecord<T>> extends Filter<T> implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends Filter<T> implements KeyOrValueTransformation.Value<T> {
    }
}
