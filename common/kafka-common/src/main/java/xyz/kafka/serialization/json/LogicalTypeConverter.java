package xyz.kafka.serialization.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

/**
 * LogicalTypeConverter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-20
 */
public interface LogicalTypeConverter {

    /**
     * object to json node
     *
     * @param schema
     * @param value
     * @param config
     * @return
     */
    JsonNode toJsonData(Schema schema, Object value, JsonDataConfig config);

    /**
     * json node to object
     *
     * @param schema
     * @param value
     * @return
     */
    Object toConnectData(Schema schema, JsonNode value);
}