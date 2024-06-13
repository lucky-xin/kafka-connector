package xyz.kafka.connector.transforms.scripting;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.jetbrains.annotations.Nullable;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;


/**
 * An implementation of the expression language evaluator based on JSR 223 scripting languages.
 * The expression receives variables to work with
 * <ul>
 * <li>key - key of the record</li>
 * <li>value - value of the record</li>
 * <li>keySchema - schema for key</li>
 * <li>valueSchema - schema for value</li>
 * </ul>
 *
 * @author Jiri Pechanec
 */
public class Jsr223Engine implements Engine {

    protected String expression;
    protected CompiledScript script;
    protected ScriptEngine engine;

    @Override
    public void configure(String language, String expression) {
        this.expression = expression;
        final ScriptEngineManager factory = new ScriptEngineManager();
        engine = factory.getEngineByName(language);
        if (engine == null) {
            throw new ConnectException("Implementation of language '" + language + "' not found on the classpath");
        }
        configureEngine();
        if (engine instanceof Compilable c) {
            try {
                script = c.compile(expression);
            } catch (ScriptException e) {
                throw new ConnectException(e);
            }
        }
    }

    protected void configureEngine() {
    }

    protected Bindings getBindings(ConnectRecord<?> r) {
        final Bindings bindings = engine.createBindings();

        bindings.put("key", key(r));
        bindings.put("value", value(r));
        bindings.put("keySchema", r.keySchema());
        bindings.put("valueSchema", r.valueSchema());
        bindings.put("topic", r.topic());
        bindings.put("header", headers(r));
        bindings.put("currTimeMills", System.currentTimeMillis());
        bindings.put("timestamp", r.timestamp());
        bindings.put("kafkaPartition", r.kafkaPartition());
        return bindings;
    }

    protected Object key(ConnectRecord<?> r) {
        return r.key();
    }

    protected Object value(ConnectRecord<?> r) {
        return r.value();
    }

    protected SchemaAndValue header(Header header) {
        return new SchemaAndValue(header.schema(), header.value());
    }

    protected Object headers(ConnectRecord<?> r) {
        return doHeaders(r);
    }

    protected Map<String, SchemaAndValue> doHeaders(ConnectRecord<?> r) {
        final Map<String, SchemaAndValue> headers = new HashMap<>();
        for (Header header : r.headers()) {
            headers.put(header.key(), header(header));
        }
        return headers;
    }

    @Override
    public <T> T eval(ConnectRecord<?> r, Class<T> type) {
        Bindings bindings = getBindings(r);
        return invoke(r, type, bindings);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected <T> T invoke(ConnectRecord<?> r, Class<T> type, Bindings bindings) {
        try {
            Object result = script != null ? script.eval(bindings) : engine.eval(expression, bindings);
            if (result == null || type.isAssignableFrom(result.getClass())) {
                return (T) result;
            } else {
                throw new ConnectException("Value '" + result + "' returned by the expression is not a " + type.getSimpleName());
            }
        } catch (Exception e) {
            throw new ConnectException("Error while evaluating expression '" + expression + "' for record '" + r + "'", e);
        }
    }
}
