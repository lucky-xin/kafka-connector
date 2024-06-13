package xyz.kafka.connector.schema;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * InvalidFieldPathException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class InvalidFieldPathException extends ConnectException {
    public InvalidFieldPathException(String s) {
        super(s);
    }

    public InvalidFieldPathException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public InvalidFieldPathException(Throwable throwable) {
        super(throwable);
    }
}
