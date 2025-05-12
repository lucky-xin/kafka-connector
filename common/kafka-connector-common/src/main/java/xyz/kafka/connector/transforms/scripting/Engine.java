package xyz.kafka.connector.transforms.scripting;

import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * The interface serves as an abstraction of expression language engine.
 *
 * @author Jiri Pechanec
 */
public interface Engine {

    /**
     * Pre-compiles the expression for repeated execution.
     * The method is called once upon the engine initialization.
     *
     * @param expression
     */
    void configure(String expression);

    /**
     * Calculate a value out of the record.
     *
     * @param cr to be used
     * @param type of the result
     * @return result of calculation
     */
    <T> T eval(ConnectRecord<?> cr, Class<T> type);
}
