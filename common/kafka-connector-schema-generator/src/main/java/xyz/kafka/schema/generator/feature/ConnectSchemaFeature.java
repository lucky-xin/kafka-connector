package xyz.kafka.schema.generator.feature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.saasquatch.jsonschemainferrer.GenericSchemaFeature;
import com.saasquatch.jsonschemainferrer.GenericSchemaFeatureInput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Comparator;

/**
 * ConnectTypeSchemaFeature
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2019-04-29 16:27
 */
public class ConnectSchemaFeature implements GenericSchemaFeature {
    private static final Comparator<JsonNode> NUM_VALUE_COMPARATOR = Comparator.comparing(JsonNode::decimalValue);

    @Nullable
    @Override
    public ObjectNode getFeatureResult(@NotNull GenericSchemaFeatureInput input) {
        return input.getSamples().stream()
                .filter(JsonNode::isNumber)
                .min(NUM_VALUE_COMPARATOR)
                .map(node -> {
                    if (node instanceof DoubleNode) {
                        ObjectNode result = JsonNodeFactory.instance.objectNode();
                        result.set("connect.type", new TextNode("float64"));
                        return result;
                    } else if (node instanceof FloatNode) {
                        ObjectNode result = JsonNodeFactory.instance.objectNode();
                        result.set("connect.type", new TextNode("float32"));
                        return result;
                    } else if (node instanceof LongNode) {
                        ObjectNode result = JsonNodeFactory.instance.objectNode();
                        result.set("connect.type", new TextNode("int64"));
                        return result;
                    } else if (node instanceof IntNode) {
                        ObjectNode result = JsonNodeFactory.instance.objectNode();
                        result.set("connect.type", new TextNode("int32"));
                        return result;
                    } else if (node instanceof ShortNode) {
                        ObjectNode result = JsonNodeFactory.instance.objectNode();
                        result.set("connect.type", new TextNode("int16"));
                        return result;
                    } else if (node instanceof DecimalNode dn) {
                        BigDecimal decimal = dn.decimalValue();
                        ObjectNode result = JsonNodeFactory.instance.objectNode();
                        result.set("connect.precision", new IntNode(decimal.precision()));
                        result.set("connect.scale", new IntNode(decimal.scale()));
                        return result;
                    }
                    return null;
                })
                .orElse(null);
    }
}
