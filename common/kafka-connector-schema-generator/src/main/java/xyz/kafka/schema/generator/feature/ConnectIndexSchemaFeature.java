package xyz.kafka.schema.generator.feature;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xyz.kafka.schema.generator.GeneratorContext;
import com.saasquatch.jsonschemainferrer.GenericSchemaFeature;
import com.saasquatch.jsonschemainferrer.GenericSchemaFeatureInput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * ConnectIndexSchemaFeature
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2019-04-29 16:27
 */
public class ConnectIndexSchemaFeature implements GenericSchemaFeature {
    @Nullable
    @Override
    public ObjectNode getFeatureResult(@NotNull GenericSchemaFeatureInput input) {
        Integer current = GeneratorContext.getCurrent();
        if (current == null) {
            return null;
        }
        ObjectNode on = JsonNodeFactory.instance.objectNode();
        on.put("connect.index", current++);
        GeneratorContext.setCurrent(current);
        return on;
    }
}
