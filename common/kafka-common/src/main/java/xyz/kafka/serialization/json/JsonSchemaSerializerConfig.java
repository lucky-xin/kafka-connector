package xyz.kafka.serialization.json;

import xyz.kafka.serialization.AbstractKafkaSchemaSerDerConf;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.schemaregistry.utils.EnumRecommender;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Locale;
import java.util.Map;

/**
 * JsonSchemaSerializerConfig
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class JsonSchemaSerializerConfig extends AbstractKafkaSchemaSerDerConf {

    public static final String FAIL_UNKNOWN_PROPERTIES = "json.fail.unknown.properties";
    public static final boolean FAIL_UNKNOWN_PROPERTIES_DEFAULT = true;
    public static final String FAIL_UNKNOWN_PROPERTIES_DOC = "Whether to fail serialization if "
            + "unknown JSON properties are encountered";

    public static final String FAIL_INVALID_SCHEMA = "json.fail.invalid.schema";
    public static final boolean FAIL_INVALID_SCHEMA_DEFAULT = true;
    public static final String FAIL_INVALID_SCHEMA_DOC = "Whether to fail serialization if the "
            + "payload does not match the schema";

    public static final String WRITE_DATES_AS_ISO8601 = "json.write.dates.iso8601";
    public static final boolean WRITE_DATES_AS_ISO8601_DEFAULT = false;
    public static final String WRITE_DATES_AS_ISO8601_DOC = "Whether to write dates as "
            + "ISO 8601 strings";

    public static final String SCHEMA_SPEC_VERSION = "json.schema.spec.version";
    public static final String SCHEMA_SPEC_VERSION_DEFAULT =
            SpecificationVersion.DRAFT_7.name().toLowerCase(Locale.ROOT);
    public static final String SCHEMA_SPEC_VERSION_DOC = "The specification version to use for JSON "
            + "schemas derived from objects, one of 'draft_4', 'draft_6', 'draft_7', or 'draft_2019_09'";

    public static final String ONEOF_FOR_NULLABLES = "json.oneof.for.nullables";
    public static final boolean ONEOF_FOR_NULLABLES_DEFAULT = true;
    public static final String ONEOF_FOR_NULLABLES_DOC = "Whether JSON schemas derived from objects "
            + "will use oneOf for nullable fields";

    public static final String JSON_INDENT_OUTPUT = "json.indent.output";
    public static final boolean JSON_INDENT_OUTPUT_DEFAULT = false;
    public static final String JSON_INDENT_OUTPUT_DOC = "Whether JSON output should be indented "
            + "(\"pretty-printed\")";

    private static final ConfigDef DEFINE = baseConfigDef()
            .define(FAIL_UNKNOWN_PROPERTIES,
                    ConfigDef.Type.BOOLEAN,
                    FAIL_UNKNOWN_PROPERTIES_DEFAULT,
                    ConfigDef.Importance.LOW,
                    FAIL_UNKNOWN_PROPERTIES_DOC
            ).define(FAIL_INVALID_SCHEMA,
                    ConfigDef.Type.BOOLEAN,
                    FAIL_INVALID_SCHEMA_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    FAIL_INVALID_SCHEMA_DOC
            ).define(WRITE_DATES_AS_ISO8601,
                    ConfigDef.Type.BOOLEAN,
                    WRITE_DATES_AS_ISO8601_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    WRITE_DATES_AS_ISO8601_DOC
            ).define(SCHEMA_SPEC_VERSION,
                    ConfigDef.Type.STRING,
                    SCHEMA_SPEC_VERSION_DEFAULT,
                    EnumRecommender.in(SpecificationVersion.values()),
                    ConfigDef.Importance.MEDIUM,
                    SCHEMA_SPEC_VERSION_DOC
            ).define(ONEOF_FOR_NULLABLES,
                    ConfigDef.Type.BOOLEAN,
                    ONEOF_FOR_NULLABLES_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    ONEOF_FOR_NULLABLES_DOC
            ).define(JSON_INDENT_OUTPUT,
                    ConfigDef.Type.BOOLEAN,
                    JSON_INDENT_OUTPUT_DEFAULT,
                    ConfigDef.Importance.LOW,
                    JSON_INDENT_OUTPUT_DOC
            );


    public JsonSchemaSerializerConfig(Map<?, ?> props) {
        super(DEFINE, props);
    }

}
