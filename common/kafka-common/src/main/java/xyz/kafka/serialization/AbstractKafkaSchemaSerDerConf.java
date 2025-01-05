package xyz.kafka.serialization;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;
import xyz.kafka.utils.ConfigUtil;

import java.util.Map;

/**
 * AbstractKafkaSchemaSerDeConfig
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2023/8/7
 */
public class AbstractKafkaSchemaSerDerConf extends AbstractKafkaSchemaSerDeConfig {

    public AbstractKafkaSchemaSerDerConf(ConfigDef config, Map<?, ?> props) {
        super(config, ConfigUtil.addDefaultEnvValue(props));
    }
}
