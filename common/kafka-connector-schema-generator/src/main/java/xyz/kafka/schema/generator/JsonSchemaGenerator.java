package xyz.kafka.schema.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xyz.kafka.schema.generator.feature.ConnectIndexSchemaFeature;
import xyz.kafka.schema.generator.feature.ConnectSchemaFeature;
import xyz.kafka.schema.generator.feature.RegexSchemaFeature;
import com.saasquatch.jsonschemainferrer.AdditionalPropertiesPolicies;
import com.saasquatch.jsonschemainferrer.ArrayLengthFeature;
import com.saasquatch.jsonschemainferrer.DefaultPolicies;
import com.saasquatch.jsonschemainferrer.EnumExtractors;
import com.saasquatch.jsonschemainferrer.ExamplesPolicies;
import com.saasquatch.jsonschemainferrer.FormatInferrer;
import com.saasquatch.jsonschemainferrer.FormatInferrers;
import com.saasquatch.jsonschemainferrer.IntegerTypeCriteria;
import com.saasquatch.jsonschemainferrer.IntegerTypePreference;
import com.saasquatch.jsonschemainferrer.JsonSchemaInferrer;
import com.saasquatch.jsonschemainferrer.JsonSchemaInferrerBuilder;
import com.saasquatch.jsonschemainferrer.NumberRangeFeature;
import com.saasquatch.jsonschemainferrer.ObjectSizeFeature;
import com.saasquatch.jsonschemainferrer.RequiredPolicies;
import com.saasquatch.jsonschemainferrer.SpecVersion;
import com.saasquatch.jsonschemainferrer.StringLengthFeature;
import com.saasquatch.jsonschemainferrer.TitleDescriptionGenerator;
import com.saasquatch.jsonschemainferrer.TitleDescriptionGeneratorInput;

import java.time.DayOfWeek;
import java.time.Month;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

/**
 * JsonSchema生成器
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2019-04-29 16:27
 */
public class JsonSchemaGenerator {

    enum BuiltInTitleDescriptionGenerators implements TitleDescriptionGenerator {

        NO_OP {},

        USE_FIELD_NAMES_AS_TITLES {
            @Override
            public String generateTitle(TitleDescriptionGeneratorInput input) {
                return input.getFieldName();
            }

            @Override
            public String generateDescription(TitleDescriptionGeneratorInput input) {
                return input.getFieldName();
            }
        }

    }

    private final JsonSchemaInferrer inferrer;

    public JsonSchemaGenerator(boolean numberRange, boolean stringLength, boolean arrayLength, List<FormatInferrer> formatInferrers) {
        JsonSchemaInferrerBuilder builder = JsonSchemaInferrer.newBuilder()
                .setSpecVersion(SpecVersion.DRAFT_2020_12)
                .setAdditionalPropertiesPolicy(AdditionalPropertiesPolicies.allowed())
                .setDefaultPolicy(DefaultPolicies.useFirstSamples())
                .setExamplesPolicy(ExamplesPolicies.useFirstSamples(1))
                .setRequiredPolicy(RequiredPolicies.nonNullCommonFields())
                .setObjectSizeFeatures(EnumSet.allOf(ObjectSizeFeature.class))
                .addFormatInferrers(
                        formatInferrers.toArray(new FormatInferrer[0])
                ).addGenericSchemaFeatures(
                        new ConnectSchemaFeature(), new ConnectIndexSchemaFeature(),
                        new RegexSchemaFeature()
                ).setTitleDescriptionGenerator(BuiltInTitleDescriptionGenerators.USE_FIELD_NAMES_AS_TITLES)
                .setIntegerTypeCriterion(IntegerTypeCriteria.nonFloatingPoint())
                .setIntegerTypePreference(IntegerTypePreference.IF_ALL)
                .addEnumExtractors(
                        EnumExtractors.validEnum(Month.class),
                        EnumExtractors.validEnum(DayOfWeek.class)
                );

        if (numberRange) {
            builder.setNumberRangeFeatures(EnumSet.allOf(NumberRangeFeature.class));
        }
        if (stringLength) {
            builder.setStringLengthFeatures(EnumSet.allOf(StringLengthFeature.class));
        }
        if (arrayLength) {
            builder.setArrayLengthFeatures(EnumSet.allOf(ArrayLengthFeature.class));
        }
        this.inferrer = builder.build();
    }

    public JsonSchemaGenerator(Consumer<JsonSchemaInferrerBuilder> consumer) {
        JsonSchemaInferrerBuilder builder = JsonSchemaInferrer.newBuilder()
                .setSpecVersion(SpecVersion.DRAFT_2020_12)
                .setAdditionalPropertiesPolicy(AdditionalPropertiesPolicies.allowed())
                .setDefaultPolicy(DefaultPolicies.useFirstSamples())
                .setExamplesPolicy(ExamplesPolicies.useFirstSamples(1))
                .setRequiredPolicy(RequiredPolicies.nonNullCommonFields())
                .setObjectSizeFeatures(EnumSet.allOf(ObjectSizeFeature.class))
                .addFormatInferrers(
                        FormatInferrers.email(), FormatInferrers.ip(),
                        FormatInferrers.dateTime(), FormatInferrers.noOp()
                ).addGenericSchemaFeatures(
                        new ConnectSchemaFeature(), new ConnectIndexSchemaFeature(),
                        new RegexSchemaFeature()
                ).setTitleDescriptionGenerator(BuiltInTitleDescriptionGenerators.USE_FIELD_NAMES_AS_TITLES)
                .setIntegerTypeCriterion(IntegerTypeCriteria.nonFloatingPoint())
                .setIntegerTypePreference(IntegerTypePreference.IF_ALL)
                .addEnumExtractors(
                        EnumExtractors.validEnum(Month.class),
                        EnumExtractors.validEnum(DayOfWeek.class)
                );
        consumer.accept(builder);
        this.inferrer = builder.build();
    }


    public ObjectNode toSchema(JsonNode jsonSource) {
        try {
            GeneratorContext.setCurrent(0);
            return this.inferrer.inferForSample(jsonSource);
        } finally {
            GeneratorContext.clear();
        }

    }
}
