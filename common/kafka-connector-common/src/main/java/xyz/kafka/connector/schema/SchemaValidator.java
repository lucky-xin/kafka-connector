package xyz.kafka.connector.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * SchemaValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@FunctionalInterface
public interface SchemaValidator {
    void validate(Schema schema, Schema schema2) throws InvalidSchemaChangeException;

    default SchemaValidator and(SchemaValidator other) {
        if (other == this || other == null) {
            return this;
        }
        return (newSchema, previous) -> {
            this.validate(newSchema, previous);
            other.validate(newSchema, previous);
        };
    }

    default SchemaValidator or(SchemaValidator other) {
        if (other == this || other == null) {
            return this;
        }
        return (newSchema, previous) -> {
            try {
                this.validate(newSchema, previous);
            } catch (ConnectException e1) {
                try {
                    other.validate(newSchema, previous);
                } catch (ConnectException e) {
                    throw e1;
                }
            }
        };
    }
}
