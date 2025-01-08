package xyz.kafka.connector.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.Requirements;
import xyz.kafka.connector.transforms.scripting.MVEL2Engine;
import xyz.kafka.connector.utils.CastUtil;
import xyz.kafka.connector.validator.Validators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/**
 * ReplaceValue
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class ReplaceValue<T extends ConnectRecord<T>> extends AbstractTransformation<T>
        implements KeyOrValueTransformation<T> {
    public static final String CONFIG_CONDITION = "condition";
    public static final String CONFIG_FIELDS = "fields";
    public static final String CONFIG_TYPE = "source.type";
    public static final String CONFIG_VALUE = "value";

    private ScriptEngine engine;
    private List<String> fieldList;
    private Object targetValue;

    private static class ScriptEngine extends MVEL2Engine {

        public <T> T eval(Object curr, ConnectRecord<?> r, Class<T> type) {
            Map<String, Object> bindings = new HashMap<>(2);
            bindings.put(CONFIG_VALUE, curr);
            return invoke(r, type, bindings);
        }
    }

    protected ReplaceValue() {
        super(new ConfigDef()
                .define(CONFIG_FIELDS,
                        ConfigDef.Type.LIST,
                        NO_DEFAULT_VALUE,
                        Validators.nonEmptyList(),
                        ConfigDef.Importance.HIGH,
                        "Name of the field will be convert to target value"
                )
                .define(CONFIG_VALUE,
                        ConfigDef.Type.STRING,
                        NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "target value"
                ).define(CONFIG_TYPE,
                        ConfigDef.Type.STRING,
                        NO_DEFAULT_VALUE,
                        Validators.oneOf("int8", "int16", "int32", "int64", "float32", "float64", "boolean", "string"),
                        ConfigDef.Importance.HIGH,
                        "type,one of:int8,int16,int32,int64,float32,float64,boolean,string"
                )
                .define(CONFIG_CONDITION,
                        ConfigDef.Type.STRING,
                        NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "An expression determining whether the record should be convert."
                )
        );
    }

    @Override
    public void configure(Map<String, ?> configs, AbstractConfig config) {
        Schema.Type type = Schema.Type.valueOf(config.getString(CONFIG_TYPE).toUpperCase());
        this.fieldList = config.getList(CONFIG_FIELDS);
        this.targetValue = CastUtil.castValueToType(null, config.getString(CONFIG_VALUE), type);
        String expression = config.getString(CONFIG_CONDITION);
        try {
            engine = new ScriptEngine();
            engine.configure(expression);
        } catch (Exception e) {
            throw new ConnectException("Failed to parse expression '" + expression + "'", e);
        }
    }

    @Override
    public T apply(T t) {
        Schema schema = schema(t);
        Struct struct = Requirements.requireStructOrNull(t.value(), t.topic());
        if (struct == null) {
            return null;
        }
        List<Field> fields = schema.fields();
        Struct newVal = new Struct(schema);
        for (Field field : fields) {
            Object val = struct.get(field);
            if (fieldList.contains(field.name())
                    && Boolean.TRUE.equals(engine.eval(val, t, Boolean.class))) {
                newVal.put(field, targetValue);
            } else {
                newVal.put(field, val);
            }
        }
        return newRecord(t, newVal, schema);
    }

    public static class Key<T extends ConnectRecord<T>> extends ReplaceValue<T>
            implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends ReplaceValue<T>
            implements KeyOrValueTransformation.Value<T> {
    }
}
