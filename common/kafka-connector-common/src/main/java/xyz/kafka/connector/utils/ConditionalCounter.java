package xyz.kafka.connector.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.LongFunction;

/**
 * ConditionalCounter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class ConditionalCounter {
    private static final Logger log = LoggerFactory.getLogger(ConditionalCounter.class);
    private long counter = 0;
    private long nextCounter;
    private final LongFunction<Long> incrementer;

    public static ConditionalCounter periodic(long interval) {
        if (interval >= 1) {
            return new ConditionalCounter(input -> {
                if (input == 0) {
                    return 1L;
                }
                if (input == 1) {
                    return interval;
                }
                return input + interval;
            });
        }
        throw new IllegalArgumentException("The interval must be positive");
    }

    public static ConditionalCounter logarithmicUntil(long maxInterval) {
        return logarithmicUntil(maxInterval, 10);
    }

    public static ConditionalCounter logarithmicUntil(long maxInterval, long factor) {
        if (maxInterval >= 1) {
            return new ConditionalCounter(input -> {
                if (input == 0) {
                    return 1L;
                }
                if (input < maxInterval) {
                    return input * factor;
                }
                return input + maxInterval;
            });
        }
        throw new IllegalArgumentException("The interval must be positive");
    }

    public static ConditionalCounter constant(long increment) {
        if (increment >= 1) {
            return new ConditionalCounter(input -> increment);
        }
        throw new IllegalArgumentException("The increment must be positive");
    }

    protected ConditionalCounter(LongFunction<Long> incrementer) {
        this.incrementer = incrementer;
        this.nextCounter = incrementer.apply(0);
    }

    public boolean incrementAndConditionally(Runnable function) {
        Objects.requireNonNull(function);
        this.counter++;
        if (this.counter != this.nextCounter) {
            return false;
        }
        this.nextCounter = this.incrementer.apply(this.nextCounter);
        try {
            function.run();
            return true;
        } catch (Throwable t) {
            log.error("Error while running conditional counter increment function", t);
            return true;
        }
    }

    public long current() {
        return this.counter;
    }

    public long next() {
        return this.nextCounter;
    }

    public void reset() {
        this.counter = 0;
        this.nextCounter = this.incrementer.apply(this.counter);
    }

    public String toString() {
        return "Counter: current=" + current() + "; next=" + next();
    }
}
