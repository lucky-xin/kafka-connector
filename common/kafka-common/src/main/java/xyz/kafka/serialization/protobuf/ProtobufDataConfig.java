package xyz.kafka.serialization.protobuf;

import io.confluent.connect.schema.AbstractDataConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

/**
 * ProtobufDataConfig
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class ProtobufDataConfig extends AbstractDataConfig {

  public static final String ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG =
      "enhanced.protobuf.schema.support";
  public static final boolean ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DEFAULT = false;
  public static final String ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DOC =
      "Toggle for enabling/disabling enhanced protobuf schema support: "
          + "package name preservation";

  public static final String SCRUB_INVALID_NAMES_CONFIG = "scrub.invalid.names";
  public static final boolean SCRUB_INVALID_NAMES_DEFAULT = false;
  public static final String SCRUB_INVALID_NAMES_DOC =
      "Whether to scrub invalid names by replacing invalid characters with valid ones";

  public static final String INT_FOR_ENUMS_CONFIG = "int.for.enums";
  public static final boolean INT_FOR_ENUMS_DEFAULT = false;
  public static final String INT_FOR_ENUMS_DOC = "Whether to represent enums as integers";

  public static final String OPTIONAL_FOR_NULLABLES_CONFIG = "optional.for.nullables";
  public static final boolean OPTIONAL_FOR_NULLABLES_DEFAULT = false;
  public static final String OPTIONAL_FOR_NULLABLES_DOC = "Whether nullable fields should be "
      + "specified with an optional label";

  public static final String WRAPPER_FOR_NULLABLES_CONFIG = "wrapper.for.nullables";
  public static final boolean WRAPPER_FOR_NULLABLES_DEFAULT = false;
  public static final String WRAPPER_FOR_NULLABLES_DOC = "Whether nullable fields should use "
      + "primitive wrapper messages";

  public static final String WRAPPER_FOR_RAW_PRIMITIVES_CONFIG = "wrapper.for.raw.primitives";
  public static final boolean WRAPPER_FOR_RAW_PRIMITIVES_DEFAULT = true;
  public static final String WRAPPER_FOR_RAW_PRIMITIVES_DOC = "Whether a wrapper message "
      + "should be interpreted as a raw primitive at the root level";

  public static final String GENERATE_STRUCT_FOR_NULLS_CONFIG = "generate.struct.for.nulls";
  public static final boolean GENERATE_STRUCT_FOR_NULLS_DEFAULT = false;
  public static final String GENERATE_STRUCT_FOR_NULLS_DOC = "Whether to generate a default struct "
      + "for null messages";

  public static final String GENERATE_INDEX_FOR_UNIONS_CONFIG = "generate.index.for.unions";
  public static final boolean GENERATE_INDEX_FOR_UNIONS_DEFAULT = true;
  public static final String GENERATE_INDEX_FOR_UNIONS_DOC = "Whether to suffix union"
      + "names with an underscore followed by an index";

  public static ConfigDef baseConfigDef() {
    return AbstractDataConfig.baseConfigDef()
        .define(ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DOC)
        .define(SCRUB_INVALID_NAMES_CONFIG, ConfigDef.Type.BOOLEAN, SCRUB_INVALID_NAMES_DEFAULT,
            ConfigDef.Importance.MEDIUM, SCRUB_INVALID_NAMES_DOC)
        .define(INT_FOR_ENUMS_CONFIG,
            ConfigDef.Type.BOOLEAN,
            INT_FOR_ENUMS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            INT_FOR_ENUMS_DOC)
        .define(OPTIONAL_FOR_NULLABLES_CONFIG,
            ConfigDef.Type.BOOLEAN,
            OPTIONAL_FOR_NULLABLES_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            OPTIONAL_FOR_NULLABLES_DOC)
        .define(WRAPPER_FOR_NULLABLES_CONFIG,
            ConfigDef.Type.BOOLEAN,
            WRAPPER_FOR_NULLABLES_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            WRAPPER_FOR_NULLABLES_DOC)
        .define(WRAPPER_FOR_RAW_PRIMITIVES_CONFIG,
            ConfigDef.Type.BOOLEAN,
            WRAPPER_FOR_RAW_PRIMITIVES_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            WRAPPER_FOR_RAW_PRIMITIVES_DOC)
        .define(GENERATE_STRUCT_FOR_NULLS_CONFIG,
            ConfigDef.Type.BOOLEAN,
            GENERATE_STRUCT_FOR_NULLS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            GENERATE_STRUCT_FOR_NULLS_DOC)
        .define(GENERATE_INDEX_FOR_UNIONS_CONFIG,
            ConfigDef.Type.BOOLEAN,
            GENERATE_INDEX_FOR_UNIONS_DEFAULT,
            ConfigDef.Importance.LOW,
            GENERATE_INDEX_FOR_UNIONS_DOC
        );
  }

  public ProtobufDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public boolean isEnhancedProtobufSchemaSupport() {
    return this.getBoolean(ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG);
  }

  public boolean isScrubInvalidNames() {
    return this.getBoolean(SCRUB_INVALID_NAMES_CONFIG);
  }

  public boolean useIntForEnums() {
    return this.getBoolean(INT_FOR_ENUMS_CONFIG);
  }

  public boolean useOptionalForNullables() {
    return this.getBoolean(OPTIONAL_FOR_NULLABLES_CONFIG);
  }

  public boolean useWrapperForNullables() {
    return this.getBoolean(WRAPPER_FOR_NULLABLES_CONFIG);
  }

  public boolean useWrapperForRawPrimitives() {
    return this.getBoolean(WRAPPER_FOR_RAW_PRIMITIVES_CONFIG);
  }

  public boolean generateStructForNulls() {
    return this.getBoolean(GENERATE_STRUCT_FOR_NULLS_CONFIG);
  }

  public boolean generateIndexForUnions() {
    return this.getBoolean(GENERATE_INDEX_FOR_UNIONS_CONFIG);
  }

  public static class Builder {

    private final Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public ProtobufDataConfig build() {
      return new ProtobufDataConfig(props);
    }
  }
}
