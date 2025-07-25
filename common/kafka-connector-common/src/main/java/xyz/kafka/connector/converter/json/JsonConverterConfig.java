package xyz.kafka.connector.converter.json;

import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;
import xyz.kafka.serialization.strategy.TopicNameStrategy;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID_DEFAULT;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID_DOC;

/**
 * JsonConverterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-10-11
 */
public class JsonConverterConfig extends ConverterConfig {

    public static final String USE_BIG_DECIMAL_FOR_FLOATS = "use.big.decimal.for.floats";
    public static final String WRITE_BIG_DECIMAL_AS_PLAIN = "write.big.decimal.as.plain";
    public static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    public static final String SCHEMA_GEN_DATE_TIME_INFER_ENABLE = "schema.gen.date.time.infer.enabled";
    public static final String SCHEMA_GEN_EMAIL_INFER_ENABLE = "schema.gen.email.infer.enabled";
    public static final String SCHEMA_GEN_IP_INFER_ENABLE = "schema.gen.ip.infer.enabled";
    private static final String SCHEMA_GROUP = "Schemas";

    public static final String SUBJECT_NAME_STRATEGY = "subject.name.strategy";

    public static ConfigDef configDef() {
        int idx = 0;
        return ConverterConfig.newConfigDef()
                .define(
                        USE_BIG_DECIMAL_FOR_FLOATS,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "use big decimal for floats",
                        SCHEMA_GROUP,
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        "use big decimal for floats"
                ).define(
                        WRITE_BIG_DECIMAL_AS_PLAIN,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        "write big decimal as plain",
                        SCHEMA_GROUP,
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        "write big decimal as plain"
                ).define(
                        AUTO_REGISTER_SCHEMAS,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "auto register schemas",
                        SCHEMA_GROUP,
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        "auto register schemas"
                ).define(
                        SUBJECT_NAME_STRATEGY,
                        ConfigDef.Type.CLASS,
                        TopicNameStrategy.class.getName(),
                        ConfigDef.Importance.LOW,
                        "subject name strategy",
                        SCHEMA_GROUP,
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        "subject name strategy"
                ).define(
                        USE_SCHEMA_ID,
                        ConfigDef.Type.INT,
                        USE_SCHEMA_ID_DEFAULT,
                        ConfigDef.Importance.LOW,
                        USE_SCHEMA_ID_DOC,
                        SCHEMA_GROUP,
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        USE_SCHEMA_ID_DOC
                ).define(
                        SCHEMA_GEN_DATE_TIME_INFER_ENABLE,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        "schema gen date time infer enable",
                        "Gen",
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        "schema gen date time infer enable"
                ).define(
                        SCHEMA_GEN_EMAIL_INFER_ENABLE,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        "schema gen email infer enable",
                        "Gen",
                        idx++,
                        ConfigDef.Width.MEDIUM,
                        "schema gen email infer enable"
                ).define(
                        SCHEMA_GEN_IP_INFER_ENABLE,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        "schema gen ip infer enable",
                        "Gen",
                        idx,
                        ConfigDef.Width.MEDIUM,
                        "schema gen ip infer enable"
                );
    }

    private final boolean autoRegisterSchemas;
    private final boolean useBigDecimalForFloats;
    private final boolean writeBigDecimalAsPlain;
    private final boolean schemaGenDateTimeInferEnable;
    private final boolean schemaGenEmailInferEnable;
    private final boolean schemaGenIpInferEnable;
    private final int useSchemaId;
    private final SubjectNameStrategy subjectNameStrategy;

    public JsonConverterConfig(Map<String, ?> props) {
        super(configDef(), props);
        this.autoRegisterSchemas = getBoolean(AUTO_REGISTER_SCHEMAS);
        this.useBigDecimalForFloats = getBoolean(USE_BIG_DECIMAL_FOR_FLOATS);
        this.writeBigDecimalAsPlain = getBoolean(WRITE_BIG_DECIMAL_AS_PLAIN);
        this.schemaGenDateTimeInferEnable = getBoolean(SCHEMA_GEN_DATE_TIME_INFER_ENABLE);
        this.schemaGenEmailInferEnable = getBoolean(SCHEMA_GEN_EMAIL_INFER_ENABLE);
        this.schemaGenIpInferEnable = getBoolean(SCHEMA_GEN_IP_INFER_ENABLE);
        this.useSchemaId = getInt(USE_SCHEMA_ID);
        this.subjectNameStrategy = this.getConfiguredInstance(SUBJECT_NAME_STRATEGY, SubjectNameStrategy.class);
        this.subjectNameStrategy.configure(originalsWithPrefix("subject.name.strategy."));
    }

    public boolean autoRegisterSchemas() {
        return autoRegisterSchemas;
    }

    public boolean useBigDecimalForFloats() {
        return useBigDecimalForFloats;
    }

    public boolean writeBigDecimalAsPlain() {
        return writeBigDecimalAsPlain;
    }

    public boolean schemaGenDateTimeInferEnable() {
        return schemaGenDateTimeInferEnable;
    }

    public boolean schemaGenEmailInferEnable() {
        return schemaGenEmailInferEnable;
    }

    public boolean schemaGenIpInferEnable() {
        return schemaGenIpInferEnable;
    }

    public int useSchemaId() {
        return useSchemaId;
    }

    public SubjectNameStrategy subjectNameStrategy() {
        return subjectNameStrategy;
    }
}
