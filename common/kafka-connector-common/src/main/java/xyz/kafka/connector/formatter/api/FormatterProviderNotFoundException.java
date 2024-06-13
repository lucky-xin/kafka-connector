package xyz.kafka.connector.formatter.api;

/**
 * FormatterProviderNotFoundException
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class FormatterProviderNotFoundException extends RuntimeException {
    public FormatterProviderNotFoundException(String s) {
        super(s);
    }

    public FormatterProviderNotFoundException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public FormatterProviderNotFoundException(Throwable throwable) {
        super(throwable);
    }
}
