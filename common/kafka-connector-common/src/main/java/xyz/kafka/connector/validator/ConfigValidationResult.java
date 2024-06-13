package xyz.kafka.connector.validator;

import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * ConfigValidationResult
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public interface ConfigValidationResult {

    interface ValidationOperation {
        void then(Runnable runnable);

        <T> Optional<T> thenReturn(Callable<T> callable);
    }

    boolean isValid(String str);

    void recordError(String str, String str2);

    default boolean isNotValid(String key) {
        return !isValid(key);
    }

    default boolean areEachValid(String... keys) {
        for (String key : keys) {
            if (!isValid(key)) {
                return false;
            }
        }
        return true;
    }

    default boolean areAnyNotValid(String... keys) {
        return !areEachValid(keys);
    }

    default void recordErrors(String message, String... keys) {
        for (String key : keys) {
            recordError(message, key);
        }
    }

    default ValidationOperation whenValid(String key) {
        if (isValid(key)) {
            return new ConfigValidationCheck(new String[]{key}, this);
        }
        return NoOpValidationCheck.INSTANCE;
    }

    default ValidationOperation whenEachAreValid(String... keys) {
        if (areEachValid(keys)) {
            return new ConfigValidationCheck(keys, this);
        }
        return NoOpValidationCheck.INSTANCE;
    }
}
