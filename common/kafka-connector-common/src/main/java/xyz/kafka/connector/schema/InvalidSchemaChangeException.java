package xyz.kafka.connector.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * InvalidSchemaChangeException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class InvalidSchemaChangeException extends ConnectException {
    public final Schema previousSchema;
    public final Schema newSchema;

    private static Schema schema(Schema schema) {
        return schema instanceof SchemaBuilder sb ? sb.build() : schema;
    }

    public InvalidSchemaChangeException(Schema previousSchema, Schema newSchema, String s) {
        super(s);
        this.previousSchema = schema(previousSchema);
        this.newSchema = schema(newSchema);
    }

    public InvalidSchemaChangeException(Schema previousSchema, Schema newSchema, String s, Throwable throwable) {
        super(s, throwable);
        this.previousSchema = schema(previousSchema);
        this.newSchema = schema(newSchema);
    }

    public InvalidSchemaChangeException(Schema previousSchema, Schema newSchema, Throwable throwable) {
        super(throwable);
        this.previousSchema = schema(previousSchema);
        this.newSchema = schema(newSchema);
    }
}
