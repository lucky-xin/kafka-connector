package xyz.kafka.connector.utils;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import xyz.kafka.connector.enums.BehaviorOnNullValues;
import xyz.kafka.connector.validator.Validators;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.between;

/**
 * ConfigUtils
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2025-07-11
 */
public class ConfigUtils {

    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a null value (i.e. Kafka tombstone records)."
            + " Valid options are 'ignore', 'fail' and 'delete'."
            + " Ignore would skip the tombstone record and fail would cause the connector task to"
            + " throw an exception."
            + " In case of the write tombstone option, the connector redirects tombstone records"
            + " to a separate directory mentioned in the config tombstone.encoded.partition."
            + " The storage of Kafka record keys is mandatory when this option is selected and"
            + " the file for values is not generated for tombstone records.";
    private static final String BEHAVIOR_ON_NULL_VALUES_DISPLAY = "Behavior for null-valued records";
    private static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = BehaviorOnNullValues.DELETE.name().toLowerCase();

    public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    public static final String ERRORS_TOLERANCE_DISPLAY = "Error Tolerance";
    public static final String ERRORS_TOLERANCE_DEFAULT = "none";
    public static final String ERRORS_TOLERANCE_DOC =
            "Behavior for tolerating errors during connector operation. 'none' is the default value "
                    + "and signals that any error will result in an immediate connector task failure; 'all' "
                    + "changes the behavior to skip over problematic records.";

    public static final String MAX_RETRIES_CONFIG = "max.retries";
    private static final String MAX_RETRIES_DOC =
            "The maximum number of retries that are allowed for failed indexing requests. If the retry "
                    + "attempts are exhausted the task will fail.";
    private static final String MAX_RETRIES_DISPLAY = "Max Retries";
    private static final int MAX_RETRIES_DEFAULT = 5;

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC =
            "How long to wait in milliseconds before attempting the first retry of a failed indexing "
                    + "request. Upon a failure, this connector may wait up to twice as long as the previous "
                    + "wait, up to the maximum number of retries. "
                    + "This avoids retrying in a tight loop under failure scenarios.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (ms)";
    private static final long RETRY_BACKOFF_MS_DEFAULT = 100;

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC =
            "Specifies how many records to attempt to batch together for insertion into the destination"
                    + " sink, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";
    private static final int BATCH_SIZE_DEFAULT = 2000;

    public static final String PROXY_HOST_CONFIG = "proxy.host";
    private static final String PROXY_HOST_DISPLAY = "Proxy Host";
    private static final String PROXY_HOST_DOC = "The address of the proxy host to connect through. "
            + "Supports the basic authentication scheme only.";
    private static final String PROXY_HOST_DEFAULT = "";

    public static final String PROXY_PORT_CONFIG = "proxy.port";
    private static final String PROXY_PORT_DISPLAY = "Proxy Port";
    private static final String PROXY_PORT_DOC = "The port of the proxy host to connect through.";
    private static final Integer PROXY_PORT_DEFAULT = 8080;

    public static final String PROXY_USERNAME_CONFIG = "proxy.username";
    private static final String PROXY_USERNAME_DISPLAY = "Proxy Username";
    private static final String PROXY_USERNAME_DOC = "The username for the proxy host.";
    private static final String PROXY_USERNAME_DEFAULT = "";

    public static final String PROXY_PASSWORD_CONFIG = "proxy.password";
    private static final String PROXY_PASSWORD_DISPLAY = "Proxy Password";
    private static final String PROXY_PASSWORD_DOC = "The password for the proxy host.";
    private static final Password PROXY_PASSWORD_DEFAULT = null;

    public static ConfigDef commonSinkConfig(ConfigDef configDef, String group, int order) {
        configDef.define(
                BEHAVIOR_ON_NULL_VALUES_CONFIG,
                ConfigDef.Type.STRING,
                BEHAVIOR_ON_NULL_VALUES_DEFAULT,
                Validators.oneOf(BehaviorOnNullValues.class),
                ConfigDef.Importance.LOW,
                BEHAVIOR_ON_NULL_VALUES_DOC,
                group,
                ++order,
                ConfigDef.Width.SHORT,
                BEHAVIOR_ON_NULL_VALUES_DISPLAY
        ).define(
                ERRORS_TOLERANCE_CONFIG,
                ConfigDef.Type.STRING,
                ERRORS_TOLERANCE_DEFAULT,
                ConfigDef.Importance.LOW,
                ERRORS_TOLERANCE_DOC,
                group,
                ++order,
                ConfigDef.Width.SHORT,
                ERRORS_TOLERANCE_DISPLAY
        ).define(
                MAX_RETRIES_CONFIG,
                ConfigDef.Type.INT,
                MAX_RETRIES_DEFAULT,
                between(0, Integer.MAX_VALUE),
                ConfigDef.Importance.LOW,
                MAX_RETRIES_DOC,
                group,
                ++order,
                ConfigDef.Width.SHORT,
                MAX_RETRIES_DISPLAY
        ).define(
                RETRY_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                RETRY_BACKOFF_MS_DEFAULT,
                between(0, TimeUnit.DAYS.toMillis(1)),
                ConfigDef.Importance.LOW,
                RETRY_BACKOFF_MS_DOC,
                group,
                ++order,
                ConfigDef.Width.SHORT,
                RETRY_BACKOFF_MS_DISPLAY
        ).define(
                BATCH_SIZE_CONFIG,
                ConfigDef.Type.INT,
                BATCH_SIZE_DEFAULT,
                between(1, 1000000),
                ConfigDef.Importance.MEDIUM,
                BATCH_SIZE_DOC,
                group,
                ++order,
                ConfigDef.Width.SHORT,
                BATCH_SIZE_DISPLAY
        );
        return configDef;
    }

    public static ConfigDef proxyConfig(ConfigDef configDef) {
        int order = 0;
        String group = "Proxy";
        configDef.define(
                PROXY_HOST_CONFIG,
                ConfigDef.Type.STRING,
                PROXY_HOST_DEFAULT,
                ConfigDef.Importance.LOW,
                PROXY_HOST_DOC,
                group,
                order++,
                ConfigDef.Width.LONG,
                PROXY_HOST_DISPLAY
        ).define(
                PROXY_PORT_CONFIG,
                ConfigDef.Type.INT,
                PROXY_PORT_DEFAULT,
                between(1, 65535),
                ConfigDef.Importance.LOW,
                PROXY_PORT_DOC,
                group,
                order++,
                ConfigDef.Width.LONG,
                PROXY_PORT_DISPLAY
        ).define(
                PROXY_USERNAME_CONFIG,
                ConfigDef.Type.STRING,
                PROXY_USERNAME_DEFAULT,
                ConfigDef.Importance.LOW,
                PROXY_USERNAME_DOC,
                group,
                order++,
                ConfigDef.Width.LONG,
                PROXY_USERNAME_DISPLAY
        ).define(
                PROXY_PASSWORD_CONFIG,
                ConfigDef.Type.PASSWORD,
                PROXY_PASSWORD_DEFAULT,
                ConfigDef.Importance.LOW,
                PROXY_PASSWORD_DOC,
                group,
                order++,
                ConfigDef.Width.LONG,
                PROXY_PASSWORD_DISPLAY
        );
        return configDef;
    }
}
