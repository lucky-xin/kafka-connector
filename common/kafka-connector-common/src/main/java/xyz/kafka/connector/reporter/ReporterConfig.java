package xyz.kafka.connector.reporter;

import cn.hutool.core.text.CharSequenceUtil;
import xyz.kafka.connector.formatter.api.AvailableFormatters;
import xyz.kafka.connector.formatter.api.Formatter;
import xyz.kafka.connector.formatter.api.Formatters;
import xyz.kafka.connector.utils.ConfigKeys;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.connector.validator.Validators;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ReporterConfig
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class ReporterConfig extends AbstractConfig {
    public static final String RESULT_PREFIX = "result.topic.";
    public static final String ERROR_PREFIX = "error.topic.";
    public static final String PRODUCER_PREFIX = "producer.";
    public static final String ADMIN_PREFIX = "admin.";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down).";
    public static final String BOOTSTRAP_SERVERS_DISPLAY = "Reporter bootstrap servers";
    public static final String RESULT_TOPIC_NAME = "result.topic.name";
    public static final String RESULT_TOPIC_NAME_DOC = "The name of the topic to produce records to after successfully processing a sink record. Use ``${connector}`` within the pattern to specify the current connector name. Leave blank to disable error reporting behavior.";
    public static final String RESULT_TOPIC_NAME_DEFAULT = "${connector}-success";
    public static final String RESULT_TOPIC_NAME_DISPLAY = "Result topic pattern";
    public static final String RESULT_REPLICATION_FACTOR_NAME = "result.topic.replication.factor";
    public static final String RESULT_REPLICATION_FACTOR_DOC = "The replication factor of the result topic when it is automatically created by this connector. This determines how many broker failures can be tolerated before data loss occurs. This should be 1 in development environments and ALWAYS at least 3 in production environments.";
    public static final short RESULT_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String RESULT_REPLICATION_FACTOR_DISPLAY = "Result topic replication factor";
    public static final String RESULT_NUM_PARTITION_NAME = "result.topic.partitions";
    public static final String RESULT_NUM_PARTITION_DOC = "The number of partitions in the result topic when it is automatically created by this connector. This number of partitions should be the same as the number of input partitions to handle the potential throughput.";
    public static final int RESULT_NUM_PARTITION_DEFAULT = 1;
    public static final String RESULT_NUM_PARTITION_DISPLAY = "Result topic partition count";
    public static final String RESULT_KEY_FORMATTER_NAME = "result.topic.key.format";
    public static final String RESULT_KEY_FORMATTER_DOC = "The format in which the result report key is serialized.";
    public static final String RESULT_KEY_FORMATTER_DEFAULT = "json";
    public static final String RESULT_KEY_FORMATTER_DISPLAY = "Result Key Format";
    public static final String RESULT_VALUE_FORMATTER_NAME = "result.topic.value.format";
    public static final String RESULT_VALUE_FORMATTER_DOC = "The format in which the result report value is serialized.";
    public static final String RESULT_VALUE_FORMATTER_DEFAULT = "json";
    public static final String RESULT_VALUE_FORMATTER_DISPLAY = "Result Value Format";
    public static final String ERROR_TOPIC_NAME = "error.topic.name";
    public static final String ERROR_TOPIC_NAME_DOC = "The name of the topic to produce records to after each unsuccessful record sink attempt. Use ``${connector}`` within the pattern to specify the current connector name. Leave blank to disable error reporting behavior.";
    public static final String ERROR_TOPIC_NAME_DEFAULT = "${connector}-error";
    public static final String ERROR_TOPIC_NAME_DISPLAY = "Error topic pattern";
    public static final String ERROR_REPLICATION_FACTOR_NAME = "error.topic.replication.factor";
    public static final String ERROR_REPLICATION_FACTOR_DOC = "The replication factor of the error topic when it is automatically created by this connector. This determines how many broker failures can be tolerated before data loss occurs. This should be 1 in development environments and ALWAYS at least 3 in production environments.";
    public static final short ERROR_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String ERROR_REPLICATION_FACTOR_DISPLAY = "Error topic replication factor";
    public static final String ERROR_NUM_PARTITION_NAME = "error.topic.partitions";
    public static final String ERROR_NUM_PARTITION_DOC = "The number of partitions in the error topic when it is automatically created by this connector. This number of partitions should be the same as the number of input partitions in order to handle the potential throughput.";
    public static final int ERROR_NUM_PARTITION_DEFAULT = 1;
    public static final String ERROR_NUM_PARTITION_DISPLAY = "Error topic partition count";
    public static final String ERROR_KEY_FORMATTER_NAME = "error.topic.key.format";
    public static final String ERROR_KEY_FORMATTER_DOC = "The format in which the error report key is serialized.";
    public static final String ERROR_KEY_FORMATTER_DEFAULT = "json";
    public static final String ERROR_KEY_FORMATTER_DISPLAY = "Error Key Format";
    public static final String ERROR_VALUE_FORMATTER_NAME = "error.topic.value.format";
    public static final String ERROR_VALUE_FORMATTER_DOC = "The format in which the error report value is serialized.";
    public static final String ERROR_VALUE_FORMATTER_DEFAULT = "json";
    public static final String ERROR_VALUE_FORMATTER_DISPLAY = "Error Value Format";
    public static final Validators.ComposeableValidator topicPatternValidator = Validators.allOf(
            Validators.optionalVariables("connector"),
            Validators.first().transform("replacing ${connector}",
                    (key, value) -> {
                        if (value instanceof String s) {
                            return s.replace("${connector}", "a");
                        }
                        return value;
                    }).then(Validators.anyOf(Validators.topicValidator(), Validators.oneOf(""))));
    public static final AvailableFormatters AVAILABLE_FORMATTERS = Formatters.availableFormatters();
    private final Map<String, Object> producerProperties;
    private final Map<String, String> resultTopicProperties;
    private final Map<String, String> errorTopicProperties;
    private final String connectorName;
    private final String resultTopicName = interpolateString(RESULT_TOPIC_NAME);
    private final String errorTopicName = interpolateString(ERROR_TOPIC_NAME);
    private final short resultReplicationFactor = getShort(RESULT_REPLICATION_FACTOR_NAME);
    private final short errorReplicationFactor = getShort(ERROR_REPLICATION_FACTOR_NAME);
    private final int resultNumPartition = getInt(RESULT_NUM_PARTITION_NAME);
    private final int errorNumPartition = getInt(ERROR_NUM_PARTITION_NAME);
    private final Formatter resultKeyFormatter = Formatters.createFormatter(this, RESULT_KEY_FORMATTER_NAME);
    private final Formatter resultValueFormatter = Formatters.createFormatter(this, RESULT_VALUE_FORMATTER_NAME);
    private final Formatter errorKeyFormatter = Formatters.createFormatter(this, ERROR_KEY_FORMATTER_NAME);
    private final Formatter errorValueFormatter = Formatters.createFormatter(this, ERROR_VALUE_FORMATTER_NAME);
    private final Map<String, Object> adminProperties = new HashMap<>();

    public ReporterConfig(String connectorName,
                          Map<String, Object> adminDefaults,
                          Map<String, Object> producerDefaults,
                          Map<String, Object> props) {
        super(config().toConfigDef(), props);
        this.connectorName = Objects.requireNonNull(connectorName);
        if (adminDefaults != null) {
            this.adminProperties.putAll(adminDefaults);
        }
        this.adminProperties.put(BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS_CONFIG));
        this.adminProperties.putAll(originalsWithPrefix(ADMIN_PREFIX));
        this.producerProperties = new HashMap<>();
        if (producerDefaults != null) {
            this.producerProperties.putAll(producerDefaults);
        }
        this.producerProperties.put(BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS_CONFIG));
        this.producerProperties.putAll(originalsWithPrefix(PRODUCER_PREFIX));
        this.resultTopicProperties = getExtraPropertiesWithPrefix(RESULT_PREFIX);
        this.errorTopicProperties = getExtraPropertiesWithPrefix(ERROR_PREFIX);
        if (isEnabled()) {
            Validators.nonEmptyList().ensureValid(BOOTSTRAP_SERVERS_CONFIG, getList(BOOTSTRAP_SERVERS_CONFIG));
        }
    }

    public static ConfigKeys config() {
        ConfigKeys configKeys = new ConfigKeys();
        addResultTopicConfigs(configKeys);
        addErrorTopicConfigs(configKeys);
        configKeys.define(BOOTSTRAP_SERVERS_CONFIG)
                .type(ConfigDef.Type.LIST)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(BOOTSTRAP_SERVERS_DOC)
                .defaultValue("")
                .displayName(BOOTSTRAP_SERVERS_DISPLAY);
        return configKeys;
    }

    public static void addResultTopicConfigs(ConfigKeys configKeys) {
        configKeys.define(RESULT_REPLICATION_FACTOR_NAME)
                .type(ConfigDef.Type.SHORT)
                .defaultValue(RESULT_REPLICATION_FACTOR_DEFAULT)
                .width(ConfigDef.Width.SHORT)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(RESULT_REPLICATION_FACTOR_DOC)
                .validator(Validators.atLeast(1))
                .displayName(RESULT_REPLICATION_FACTOR_DISPLAY);
        Formatters.ConfigKeyBuilder.withName(RESULT_VALUE_FORMATTER_NAME)
                .availableFormatters(AVAILABLE_FORMATTERS)
                .defaultFormat(ERROR_VALUE_FORMATTER_DEFAULT)
                .width(ConfigDef.Width.LONG)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(RESULT_VALUE_FORMATTER_DOC)
                .displayName(RESULT_VALUE_FORMATTER_DISPLAY)
                .build(configKeys);
    }

    public static void addErrorTopicConfigs(ConfigKeys configKeys) {
        configKeys.define(ERROR_TOPIC_NAME)
                .type(ConfigDef.Type.STRING)
                .defaultValue(ERROR_TOPIC_NAME_DEFAULT)
                .width(ConfigDef.Width.MEDIUM)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(ERROR_TOPIC_NAME_DOC)
                .validator(topicPatternValidator)
                .displayName(ERROR_TOPIC_NAME_DISPLAY);
        configKeys.define(ERROR_NUM_PARTITION_NAME)
                .type(ConfigDef.Type.INT)
                .defaultValue(ERROR_NUM_PARTITION_DEFAULT)
                .width(ConfigDef.Width.SHORT)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(ERROR_NUM_PARTITION_DOC)
                .validator(Validators.atLeast(ERROR_NUM_PARTITION_DEFAULT))
                .displayName(ERROR_NUM_PARTITION_DISPLAY);
        Formatters.ConfigKeyBuilder.withName(ERROR_KEY_FORMATTER_NAME)
                .availableFormatters(AVAILABLE_FORMATTERS)
                .defaultFormat(ERROR_KEY_FORMATTER_DEFAULT)
                .width(ConfigDef.Width.LONG)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(ERROR_KEY_FORMATTER_DOC)
                .displayName(ERROR_KEY_FORMATTER_DISPLAY)
                .build(configKeys);
        Formatters.ConfigKeyBuilder.withName(ERROR_VALUE_FORMATTER_NAME)
                .availableFormatters(AVAILABLE_FORMATTERS)
                .defaultFormat(ERROR_VALUE_FORMATTER_DEFAULT)
                .width(ConfigDef.Width.LONG)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(ERROR_VALUE_FORMATTER_DOC)
                .displayName(ERROR_VALUE_FORMATTER_DISPLAY)
                .build(configKeys);
    }

    public String resultTopic() {
        return this.resultTopicName;
    }

    public String errorTopic() {
        return this.errorTopicName;
    }

    public boolean resultReportingEnabled() {
        return !CharSequenceUtil.isEmpty(this.resultTopicName);
    }

    public boolean errorReportingEnabled() {
        return !CharSequenceUtil.isEmpty(this.errorTopicName);
    }

    public boolean isEnabled() {
        return resultReportingEnabled() || errorReportingEnabled();
    }

    public short resultTopicReplicationFactor() {
        return this.resultReplicationFactor;
    }

    public short errorTopicReplicationFactor() {
        return this.errorReplicationFactor;
    }

    public int resultTopicPartitionCount() {
        return this.resultNumPartition;
    }

    public int errorTopicPartitionCount() {
        return this.errorNumPartition;
    }

    public Map<String, Object> adminProperties() {
        return this.adminProperties;
    }

    public Map<String, Object> producerProperties() {
        return this.producerProperties;
    }

    public Map<String, String> resultTopicProperties() {
        return this.resultTopicProperties;
    }

    public Map<String, String> errorTopicProperties() {
        return this.errorTopicProperties;
    }

    public Formatter resultKeyConverter() {
        return this.resultKeyFormatter;
    }

    public Formatter resultValueConverter() {
        return this.resultValueFormatter;
    }

    public Formatter errorKeyConverter() {
        return this.errorKeyFormatter;
    }

    public Formatter errorValueConverter() {
        return this.errorValueFormatter;
    }

    private String interpolateString(String key) {
        assert this.connectorName != null;
        return getString(key).replace("${connector}", this.connectorName);
    }

    private Map<String, String> getExtraPropertiesWithPrefix(String prefix) {
        ConfigKeys knownConfigs = config();
        return originalsWithPrefix(prefix).entrySet().stream()
                .filter(e -> !knownConfigs.contains(prefix + e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }
}
