package xyz.kafka.schema.generator;

import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * ConnectIndexSchemaFeature
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2019-04-29 16:27
 */
public class GeneratorContext {
    private static final TransmittableThreadLocal<Integer> LOCAL = new TransmittableThreadLocal<>();

    public static Integer getCurrent() {
        return LOCAL.get();
    }

    public static void setCurrent(Integer v) {
        LOCAL.set(v);
    }


    public static void clear() {
        LOCAL.remove();
    }
}
