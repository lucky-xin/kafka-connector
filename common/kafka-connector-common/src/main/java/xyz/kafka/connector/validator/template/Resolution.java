package xyz.kafka.connector.validator.template;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Resolution
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public final class Resolution<T> {
    private static final Object UNRESOLVED_VALUE = new Object();
    public static final Resolution<?> UNRESOLVED = new Resolution<>(UNRESOLVED_VALUE);
    private final T value;

    @SuppressWarnings("unchecked")
    public static <T> Resolution<T> unresolved() {
        return (Resolution<T>) UNRESOLVED;
    }

    public static <T> Resolution<T> with(T value) {
        return new Resolution<>(value);
    }

    private Resolution() {
        this.value = null;
    }

    private Resolution(T value) {
        this.value = value;
    }

    public boolean isResolved() {
        return this.value != UNRESOLVED_VALUE;
    }

    public void ifResolved(Consumer<? super T> consumer) {
        if (isResolved()) {
            consumer.accept(this.value);
        }
    }

    public T get() {
        if (isResolved()) {
            return this.value;
        }
        throw new NoSuchElementException("No value present");
    }

    public T orElse(T other) {
        return isResolved() ? this.value : other;
    }

    public T orElseGet(Supplier<? extends T> other) {
        return isResolved() ? this.value : (T) other.get();
    }

    public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws Throwable {
        if (isResolved()) {
            return this.value;
        }
        throw exceptionSupplier.get();
    }

    public <U> Resolution<U> map(Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(mapper);
        if (!isResolved()) {
            return unresolved();
        }
        return with(mapper.apply(this.value));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Resolution)) {
            return false;
        }
        return Objects.equals(this.value, ((Resolution<?>) obj).value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.value);
    }

    @Override
    public String toString() {
        return isResolved() ? String.format("Resolution[%s]", this.value) : "Resolution.unresolved";
    }
}
