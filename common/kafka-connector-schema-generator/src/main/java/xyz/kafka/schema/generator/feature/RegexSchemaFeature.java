package xyz.kafka.schema.generator.feature;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.saasquatch.jsonschemainferrer.GenericSchemaFeature;
import com.saasquatch.jsonschemainferrer.GenericSchemaFeatureInput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * RegexSchemaFeature
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2019-04-29 16:27
 */
public class RegexSchemaFeature implements GenericSchemaFeature {
    @Nullable
    @Override
    public ObjectNode getFeatureResult(@NotNull GenericSchemaFeatureInput input) {
        if (!"string".equals(input.getType())) {
            return null;
        }
        return JsonNodeFactory.instance.objectNode()
                .put("regex", ".*");
    }
}
