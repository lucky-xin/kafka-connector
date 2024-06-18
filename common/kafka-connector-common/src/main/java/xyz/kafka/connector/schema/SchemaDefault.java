package xyz.kafka.connector.schema;

import org.apache.kafka.connect.data.Schema;

/**
 * SchemaDefault
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@FunctionalInterface
public interface SchemaDefault {
    Object NONE = new Object() {
        @Override
        public String toString() {
            return "<NONE>";
        }
    };

    /**
     * computeDefault
     *
     * @param fieldPath
     * @param schema
     * @return
     */
    Object computeDefault(FieldPath fieldPath, Schema schema);

    default SchemaDefault or(SchemaDefault other) {
        if (other == this || other == null) {
            return this;
        }
        return (path, type) -> {
            Object result = this.computeDefault(path, type);
            if (result == null) {
                result = other.computeDefault(path, type);
            }
            return result;
        };
    }
}
