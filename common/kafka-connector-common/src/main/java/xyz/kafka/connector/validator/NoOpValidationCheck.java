package xyz.kafka.connector.validator;

import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * NoOpValidationCheck
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class NoOpValidationCheck implements ConfigValidationResult.ValidationOperation {
    static final NoOpValidationCheck INSTANCE = new NoOpValidationCheck();

    @Override
    public void then(Runnable check) {
    }

    @Override
    public <T> Optional<T> thenReturn(Callable<T> check) {
        return Optional.empty();
    }
}
