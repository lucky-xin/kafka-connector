package xyz.kafka.connector.transforms;

import cn.hutool.core.text.CharSequenceUtil;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import xyz.kafka.connector.transforms.scripting.Engine;
import xyz.kafka.connector.transforms.scripting.MVEL2Engine;
import xyz.kafka.connector.validator.Validators;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * This is a base class for any SMT using scripting languages.
 * Current implementation supports only JSR223 scripting languages.<p/>
 * The SMT will instantiate an scripting engine encapsulated in {@code Engine} interface in configure phase.
 * It will try to pre-parse the expression if it is allowed by the engine and than the expression is evaluated
 * for every record incoming.<p>
 * The engine will extract key, value and its schemas and will inject them as variables into the engine.
 * The mapping is unique for each expression language.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public abstract class ScriptingTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String CONDITION = "condition";
    public static final String TOPIC_REGEX = "topic.regex";
    public static final String NULL_HANDLING_MODE = "null.handling.mode";

    protected ConfigDef configDef = new ConfigDef()
            .define(
                    TOPIC_REGEX,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.HIGH,
                    "A regex used for selecting the topic(s) to which this transformation should be applied.",
                    "Scripting",
                    0,
                    ConfigDef.Width.MEDIUM,
                    "Topic regex"
            ).define(
                    NULL_HANDLING_MODE,
                    ConfigDef.Type.STRING,
                    NullHandling.DROP.value,
                    Validators.oneOf(NullHandling.class),
                    ConfigDef.Importance.HIGH,
                    "How to handle records with null value. Options are: "
                            + "keep - records are passed (the default),"
                            + "drop - records are removed,"
                            + "evaluate - the null records are passed for evaluation.",
                    "Scripting",
                    1,
                    ConfigDef.Width.MEDIUM,
                    "Handle null records"
            ).define(
                    CONDITION,
                    ConfigDef.Type.STRING,
                    null,
                    Validators.nonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "An expression determining whether the record should be filtered out. When evaluated to true the record is removed.",
                    "Scripting",
                    2,
                    ConfigDef.Width.MEDIUM,
                    "Filtering condition"
            );

    @Getter
    public enum NullHandling {
        /**
         *
         */
        DROP("drop"),
        KEEP("keep"),
        EVALUATE("evaluate");

        private final String value;

        NullHandling(String value) {
            this.value = value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static NullHandling parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (NullHandling option : NullHandling.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static NullHandling parse(String value, String defaultValue) {
            NullHandling mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }


    protected Engine engine;
    private NullHandling nullHandling;
    private Pattern topicPattern;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(configDef, configs);
        String expression = config.getString(CONDITION);
        try {
            engine = new MVEL2Engine();
            engine.configure(expression);
        } catch (Exception e) {
            throw new ConnectException("Failed to parse expression '" + expression + "'", e);
        }
        nullHandling = NullHandling.parse(config.getString(NULL_HANDLING_MODE));
        String topicRegex = config.getString(TOPIC_REGEX);
        if (CharSequenceUtil.isNotEmpty(topicRegex)) {
            this.topicPattern = Pattern.compile(topicRegex);
        }
    }

    @Override
    public R apply(R r) {
        if (topicPattern != null && !topicPattern.matcher(r.topic()).matches()) {
            return r;
        }

        if (r.value() == null) {
            if (nullHandling == NullHandling.KEEP) {
                return r;
            } else if (nullHandling == NullHandling.DROP) {
                return null;
            }
        }
        return doApply(r);
    }

    /**
     * apply
     *
     * @param r
     * @return
     */
    protected abstract R doApply(R r);

    @Override
    public ConfigDef config() {
        return configDef;
    }

    @Override
    public void close() {
    }
}

