package xyz.kafka.serialization.protobuf;

import xyz.kafka.serialization.AbstractKafkaSchemaSerDerConf;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;


/**
 * ProtobufSerializerConfig
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class ProtobufSerializerConfig extends AbstractKafkaSchemaSerDerConf {

    public static final String REFERENCE_LOOKUP_ONLY_CONFIG =
            "reference.lookup.only";
    public static final String REFERENCE_LOOKUP_ONLY_DOC =
            "Regardless of whether auto.register.schemas or use.latest.version is true, only look up "
                    + "the ID for references by using the schema.";
    public static final String SKIP_KNOWN_TYPES_CONFIG =
            "skip.known.types";
    public static final String SKIP_KNOWN_TYPES_DOC =
            "Whether to skip known types when resolving schema dependencies.";

    public static final String REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG =
            "reference.subject.name.strategy";
    public static final String REFERENCE_SUBJECT_NAME_STRATEGY_DOC =
            "Determines how to construct the subject name for referenced schemas. "
                    + "By default, the reference name is used as subject.";

    private static final ConfigDef config = baseConfigDef()
            .define(REFERENCE_LOOKUP_ONLY_CONFIG, ConfigDef.Type.BOOLEAN,
                    false, ConfigDef.Importance.MEDIUM,
                    REFERENCE_LOOKUP_ONLY_DOC)
            .define(SKIP_KNOWN_TYPES_CONFIG, ConfigDef.Type.BOOLEAN,
                    true, ConfigDef.Importance.LOW,
                    SKIP_KNOWN_TYPES_DOC)
            .define(REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG, ConfigDef.Type.CLASS,
                    DefaultReferenceSubjectNameStrategy.class, ConfigDef.Importance.LOW,
                    REFERENCE_SUBJECT_NAME_STRATEGY_DOC);

    public ProtobufSerializerConfig(Map<?, ?> props) {
        super(config, props);
    }

    public boolean onlyLookupReferencesBySchema() {
        return this.getBoolean(REFERENCE_LOOKUP_ONLY_CONFIG);
    }

    public boolean skipKnownTypes() {
        return getBoolean(SKIP_KNOWN_TYPES_CONFIG);
    }

    public ReferenceSubjectNameStrategy referenceSubjectNameStrategyInstance() {
        return this.getConfiguredInstance(REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG,
                ReferenceSubjectNameStrategy.class);
    }
}
