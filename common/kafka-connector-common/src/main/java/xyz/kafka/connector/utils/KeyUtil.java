package xyz.kafka.connector.utils;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

/**
 * Key
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-02
 */
public class KeyUtil {

    public static String convertKey(Schema keySchema, Object key) {
        if (key == null) {
            throw new DataException("Key is used as document id and can not be null.");
        }
        if (String.valueOf(key).isEmpty()) {
            throw new DataException("Key is used as document id and can not be empty.");
        }

        Schema.Type schemaType;
        Field idField = null;
        if (keySchema == null) {
            schemaType = ConnectSchema.schemaType(key.getClass());
            if (schemaType == null) {
                throw new DataException(
                        "Java class " + key.getClass() + " does not have corresponding schema type."
                );
            }
        } else {
            schemaType = keySchema.type();
            idField = keySchema.field("id");
        }
        return switch (schemaType) {
            case INT8, INT16, INT32, INT64, STRING -> String.valueOf(key);
            case STRUCT -> {
                if (idField != null && key instanceof Struct struct) {
                    yield String.valueOf(struct.get(idField));
                }
                throw new DataException(schemaType.name() + " is not supported as the document id.");
            }
            default -> throw new DataException(schemaType.name() + " is not supported as the document id.");
        };
    }

    public static String key(String index, String id) {
        return index + ":" + id;
    }
}
