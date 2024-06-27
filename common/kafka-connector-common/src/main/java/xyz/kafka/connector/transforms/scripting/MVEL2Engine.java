package xyz.kafka.connector.transforms.scripting;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.jetbrains.annotations.Nullable;
import org.mvel2.MVEL;

import java.io.Serializable;
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
public class MVEL2Engine implements Engine {

    protected String expression;
    protected Serializable compiled;

    @Override
    public void configure(String expression) {
        this.expression = expression;
        configureEngine();
        this.compiled = MVEL.compileExpression(expression);
    }


    protected void configureEngine() {
    }

    protected Map<String, Object> getBindings(ConnectRecord<?> r) {
        final Map<String, Object> bindings = new HashMap<>(16);
        bindings.put("k", key(r));
        bindings.put("v", value(r));
        bindings.put("ks", r.keySchema());
        bindings.put("vs", r.valueSchema());
        bindings.put("topic", r.topic());
        bindings.put("header", headers(r));
        bindings.put("currMills", System.currentTimeMillis());
        bindings.put("timestamp", r.timestamp());
        bindings.put("partition", r.kafkaPartition());
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
        Headers headers1 = r.headers();
        final Map<String, SchemaAndValue> headers = new HashMap<>(headers1.size());
        for (Header header : headers1) {
            headers.put(header.key(), header(header));
        }
        return headers;
    }

    @Override
    public <T> T eval(ConnectRecord<?> r, Class<T> type) {
        Map<String, Object> bindings = getBindings(r);
        return invoke(r, type, bindings);
    }

    @Nullable
    protected <T> T invoke(ConnectRecord<?> r, Class<T> type, Map<String, Object> bindings) {
        try {
            return MVEL.executeExpression(compiled, bindings, type);
        } catch (Exception e) {
            throw new ConnectException("Error while evaluating expression '" + expression + "' for record '" + r + "'", e);
        }
    }
}
