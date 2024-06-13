package xyz.kafka.connector.validator;

import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * ConfigValidationCheck
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class ConfigValidationCheck implements ConfigValidationResult.ValidationOperation {
    private final String[] keys;
    private final ConfigValidationResult result;

    public ConfigValidationCheck(String[] keys, ConfigValidationResult result) {
        this.keys = keys;
        this.result = result;
    }

    @Override
    public void then(Runnable check) {
        try {
            check.run();
        } catch (Exception e) {
            this.result.recordErrors(e.getMessage(), this.keys);
        }
    }

    @Override
    public <T> Optional<T> thenReturn(Callable<T> check) {
        try {
            return Optional.of(check.call());
        } catch (Exception e) {
            this.result.recordErrors(e.getMessage(), this.keys);
            return Optional.empty();
        }
    }
}
