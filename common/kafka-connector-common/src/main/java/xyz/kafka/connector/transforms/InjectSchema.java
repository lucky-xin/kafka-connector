package xyz.kafka.connector.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Injects a schema into a schemaless record.
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class InjectSchema<T extends ConnectRecord<T>> extends AbstractTransformation<T> implements KeyOrValueTransformation<T> {

    private static final Logger LOG = LoggerFactory.getLogger(InjectSchema.class);

    private static final String CONFIG_FIELD = "schema";

    private Schema schema;

    protected InjectSchema() {
        super(new ConfigDef()
                .define(CONFIG_FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Schema to inject. The schema is expected to be in JSON Schema format.")
        );
    }

    @Override
    public void configure(Map<String, ?> configs, AbstractConfig config) {
        String input = config.getString(CONFIG_FIELD);
        ObjectMapper mapper = new ObjectMapper();
        try (JsonConverter converter = new JsonConverter()) {
            JsonNode jsonSchema = mapper.readTree(input);
            converter.configure(Collections.singletonMap("converter.type", "value"));
            this.schema = converter.asConnectSchema(jsonSchema);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * A schemaless JSON objects are stored as a Map whereas those with schema use Struct.
     * Therefore, if the top-level object is a Map and we inject a schema this method converts the Map
     * into Struct for the object to match the schema.
     *
     */
    private Object convertTopLevelObjectToMatchSchema(Schema schema, Object value) {
        if (value instanceof Map && schema.type() == Schema.Type.STRUCT) {
            return Utils.mapToStruct(Utils.cast(value), schema);
        }

        return value;
    }

    @Override
    public T apply(T t) {
        LOG.debug("Injecting schema to record {}", t);
        final Object value = convertTopLevelObjectToMatchSchema(this.schema, value(t));
        return newRecord(t, value, schema);
    }

    public static class Key<T extends ConnectRecord<T>> extends InjectSchema<T> implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends InjectSchema<T> implements KeyOrValueTransformation.Value<T> {
    }
}
