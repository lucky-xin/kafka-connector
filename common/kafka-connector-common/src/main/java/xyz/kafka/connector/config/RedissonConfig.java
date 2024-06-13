package xyz.kafka.connector.config;

import cn.hutool.core.text.CharPool;
import cn.hutool.core.text.StrPool;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.jetbrains.annotations.NotNull;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.BaseRedisNodes;
import org.redisson.api.redisnode.RedisCluster;
import org.redisson.api.redisnode.RedisMaster;
import org.redisson.api.redisnode.RedisMasterSlave;
import org.redisson.api.redisnode.RedisNode;
import org.redisson.api.redisnode.RedisNodes;
import org.redisson.api.redisnode.RedisSentinelMasterSlave;
import org.redisson.api.redisnode.RedisSingle;
import org.redisson.config.BaseConfig;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.config.ReadMode;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import xyz.kafka.connector.enums.RedisClientType;
import xyz.kafka.connector.validator.Validators;
import xyz.kafka.utils.StringUtil;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RedissonConfig
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2023/9/4
 */
public class RedissonConfig extends AbstractConfig {

    public static final String REDIS_PROTOCOL_PREFIX = "redis://";
    public static final String REDISS_PROTOCOL_PREFIX = "rediss://";
    public static final String REDIS_NODES = "redis.nodes";
    public static final String REDIS_CLIENT_TYPE = "redis.client.type";
    public static final String REDIS_SENTINEL_MASTER_NAME = "redis.sentinel.master.name";
    public static final String REDIS_USE_SSL = "redis.use_ssl";
    public static final String REDIS_USER = "redis.user";
    public static final String REDIS_CLIENT_NAME = "redis.client.name";
    public static final String REDIS_PWD = "redis.password";
    public static final String REDIS_DB = "redis.database";
    public static final String REDIS_IDLE_CONNECTION_TIMEOUT = "redis.idle.connection.timeout.ms";
    public static final String REDIS_CONNECT_TIMEOUT = "redis.connect.timeout.ms";
    public static final String REDIS_TIMEOUT = "redis.timeout.ms";
    public static final String REDIS_RETRY_ATTEMPTS = "redis.retry.attempts";
    public static final String REDIS_RETRY_INTERVAL = "redis.retry.interval.ms";
    public static final String REDIS_SSL_PROVIDER = "redis.ssl.provider";
    public static final String REDIS_SSL_KEYSTORE_PATH = "redis.ssl.keystore.path";
    public static final String REDIS_SSL_TRUSTSTORE_PATH = "redis.ssl.truststore.path";
    public static final String REDIS_SSL_TRUSTSTORE_PASSWORD = "redis.ssl.truststore.password";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(REDIS_NODES,
                    ConfigDef.Type.LIST,
                    StringUtil.toString(System.getenv("REDIS_NODES"), "127.0.0.1:6379"),
                    Validators.nonEmptyList(),
                    ConfigDef.Importance.MEDIUM,
                    "Redis nodes."
            ).define(
                    REDIS_USER,
                    ConfigDef.Type.STRING,
                    System.getenv("REDIS_USER"),
                    ConfigDef.Importance.MEDIUM,
                    "Redis user."
            ).define(
                    REDIS_PWD,
                    ConfigDef.Type.PASSWORD,
                    System.getenv("REDIS_PWD"),
                    ConfigDef.Importance.MEDIUM,
                    "Redis password."
            ).define(
                    REDIS_DB,
                    ConfigDef.Type.INT,
                    StringUtil.toInteger(System.getenv("REDIS_DB"), 9),
                    ConfigDef.Importance.MEDIUM,
                    "Redis database."
            ).define(
                    REDIS_USE_SSL,
                    ConfigDef.Type.BOOLEAN,
                    StringUtil.toBoolean(System.getenv("REDIS_USE_SSL"), false),
                    ConfigDef.Importance.MEDIUM,
                    "Use ssl connection"
            ).define(
                    REDIS_CLIENT_TYPE,
                    ConfigDef.Type.STRING,
                    StringUtil.toString(System.getenv("REDIS_CLIENT_TYPE"), "STANDALONE"),
                    Validators.oneOfUppercase(RedisClientType.class),
                    ConfigDef.Importance.MEDIUM,
                    "Redis client type validate value is one of:STANDALONE,CLUSTER,SENTINEL,MASTER_SLAVE"
            ).define(
                    REDIS_SENTINEL_MASTER_NAME,
                    ConfigDef.Type.STRING,
                    System.getenv("REDIS_SENTINEL_MASTER_NAME"),
                    ConfigDef.Importance.MEDIUM,
                    "redis sentinel master name"
            ).define(
                    REDIS_CLIENT_NAME,
                    ConfigDef.Type.STRING,
                    StringUtil.toString(System.getenv("REDIS_CLIENT_NAME"), "Kafka Connector"),
                    ConfigDef.Importance.MEDIUM,
                    "Redis client name"
            ).define(
                    REDIS_IDLE_CONNECTION_TIMEOUT,
                    ConfigDef.Type.INT,
                    StringUtil.toInteger(System.getenv("REDIS_IDLE_CONNECTION_TIMEOUT"), 10000),
                    ConfigDef.Importance.MEDIUM,
                    "redis idle connection timeout"
            ).define(
                    REDIS_CONNECT_TIMEOUT,
                    ConfigDef.Type.INT,
                    StringUtil.toInteger(System.getenv("REDIS_CONNECT_TIMEOUT"), 10000),
                    ConfigDef.Importance.MEDIUM,
                    "redis connection timeout"
            ).define(
                    REDIS_TIMEOUT,
                    ConfigDef.Type.INT,
                    StringUtil.toInteger(System.getenv("REDIS_TIMEOUT"), 3000),
                    ConfigDef.Importance.MEDIUM,
                    "redis timeout"
            ).define(
                    REDIS_RETRY_ATTEMPTS,
                    ConfigDef.Type.INT,
                    StringUtil.toInteger(System.getenv("REDIS_RETRY_ATTEMPTS"), 3),
                    ConfigDef.Importance.MEDIUM,
                    "redis retry attempts"
            ).define(
                    REDIS_RETRY_INTERVAL,
                    ConfigDef.Type.INT,
                    StringUtil.toInteger(System.getenv("REDIS_RETRY_INTERVAL"), 1500),
                    ConfigDef.Importance.MEDIUM,
                    "redis retry interval"
            ).define(
                    REDIS_SSL_PROVIDER,
                    ConfigDef.Type.LIST,
                    StringUtil.toString(System.getenv("REDIS_SSL_PROVIDER"), "OPENSSL,JDK"),
                    Validators.emptyOrIn("OPENSSL", "JDK"),
                    ConfigDef.Importance.MEDIUM,
                    "The SSL provider to use."
            ).define(
                    REDIS_SSL_KEYSTORE_PATH,
                    ConfigDef.Type.STRING,
                    StringUtil.toString(System.getenv("REDIS_SSL_KEYSTORE_PATH")),
                    ConfigDef.Importance.MEDIUM,
                    "The path to the SSL keystore."
            ).define(
                    REDIS_SSL_TRUSTSTORE_PATH,
                    ConfigDef.Type.STRING,
                    StringUtil.toString(System.getenv("REDIS_SSL_TRUSTSTORE_PATH")),
                    ConfigDef.Importance.MEDIUM,
                    "The path to the SSL truststore."
            ).define(
                    REDIS_SSL_TRUSTSTORE_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    StringUtil.toString(System.getenv("REDIS_SSL_TRUSTSTORE_PASSWORD")),
                    ConfigDef.Importance.MEDIUM,
                    "The password for the SSL truststore."
            );

    protected final RedissonClient redisson;
    protected final RedisClientType redisClientType;

    public RedissonConfig(Map<?, ?> originals) {
        this(CONFIG_DEF, originals);
    }

    public RedissonConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, false);
        this.redisClientType = RedisClientType.valueOf(getString(REDIS_CLIENT_TYPE));
        try {
            Config c = getRedissonConfig();
            this.redisson = Redisson.create(c);
        } catch (MalformedURLException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * 获取Redisson配置。
     * 此方法根据配置的Redis客户端类型（独立、集群、哨兵、主从）来构建相应的Redisson配置。
     *
     * @return Config Redisson的配置对象。
     * @throws MalformedURLException 如果Redis节点地址格式不正确，则抛出此异常。
     */
    public Config getRedissonConfig() throws MalformedURLException {
        // 获取Redis客户端名称、数据库编号、协议前缀、Redis节点列表
        String redisClientName = getString(REDIS_CLIENT_NAME);
        Integer database = getInt(REDIS_DB);
        String prefix = Boolean.TRUE.equals(getBoolean(REDIS_USE_SSL))
                ? REDISS_PROTOCOL_PREFIX
                : REDIS_PROTOCOL_PREFIX;
        List<String> redisNodes = getList(REDIS_NODES);
        Config c = new Config();
        // 根据Redis客户端类型，配置不同的Redisson连接方式
        switch (this.redisClientType) {
            case STANDALONE -> { // 配置独立Redis服务器
                SingleServerConfig singleConfig = c.useSingleServer()
                        .setDatabase(database);
                singleConfig.setAddress(toNode(redisNodes.get(0), prefix));
                baseConfig(singleConfig);
                // 更新配置为实际解析后的Redis服务器地址
                singleConfig.setAddress(toNode(getNodes(c, RedisNodes.SINGLE).get(0).getAddr(), prefix));
            }

            case CLUSTER -> { // 配置Redis集群
                ClusterServersConfig clusterConfig = c.useClusterServers()
                        .setReadMode(ReadMode.MASTER_SLAVE)
                        .setClientName(redisClientName)
                        .setScanInterval(5000);
                clusterConfig.setNodeAddresses(toNodes(redisNodes, prefix));
                baseConfig(clusterConfig);
                // 更新配置为实际解析后的Redis集群节点地址
                clusterConfig.setNodeAddresses(
                        getNodes(c, RedisNodes.CLUSTER)
                                .stream()
                                .map(RedisNode::getAddr)
                                .map(t -> toNode(t, prefix))
                                .toList()
                );
            }

            case SENTINEL -> { // 配置Redis哨兵模式
                SentinelServersConfig sentinelConfig = c.useSentinelServers();
                sentinelConfig.setSentinelAddresses(toNodes(redisNodes, prefix));
                baseConfig(sentinelConfig);
                sentinelConfig.setMasterName(getString(REDIS_SENTINEL_MASTER_NAME))
                        .setClientName(redisClientName)
                        .setDatabase(database)
                        .setScanInterval(5000);

                // 更新配置为实际解析后的Redis哨兵和主从服务器地址
                sentinelConfig.setSentinelAddresses(
                        getNodes(c, RedisNodes.SENTINEL_MASTER_SLAVE)
                                .stream()
                                .map(RedisNode::getAddr)
                                .map(t -> toNode(t.getHostName(), t.getPort(), prefix))
                                .toList()
                );
            }

            case MASTER_SLAVE -> { // 配置Redis主从模式
                MasterSlaveServersConfig masterSlaveConfig = c.useMasterSlaveServers()
                        .setDatabase(database)
                        .setClientName(redisClientName)
                        .setReadMode(ReadMode.SLAVE);
                masterSlaveConfig.setMasterAddress(toNode(redisNodes.get(0), prefix));
                baseConfig(masterSlaveConfig);
                // 更新配置为实际解析后的Redis主从服务器地址
                List<RedisNode> nodes = getNodes(c, RedisNodes.MASTER_SLAVE);
                for (RedisNode node : nodes) {
                    InetSocketAddress addr = node.getAddr();
                    if (node instanceof RedisMaster) {
                        masterSlaveConfig.setMasterAddress(toNode(addr.getHostName(), addr.getPort(), prefix));
                        continue;
                    }
                    masterSlaveConfig.addSlaveAddress(toNode(addr.getHostName(), addr.getPort(), prefix));
                }
            }
            default -> {
            }
        }
        return c;
    }

    private <T extends BaseConfig<T>> void baseConfig(BaseConfig<T> singleConfig) throws MalformedURLException {
        String redisUser = getString(REDIS_USER);
        String redisClientName = getString(REDIS_CLIENT_NAME);
        String redisPwd = getPassword(REDIS_PWD).value();
        int idleConnectionTimeout = getInt(REDIS_IDLE_CONNECTION_TIMEOUT);
        int connectTimeout = getInt(REDIS_CONNECT_TIMEOUT);
        int timeout = getInt(REDIS_TIMEOUT);
        int retryAttempts = getInt(REDIS_RETRY_ATTEMPTS);
        int retryInterval = getInt(REDIS_RETRY_INTERVAL);
        boolean enableSsl = getBoolean(REDIS_USE_SSL);
        singleConfig
                .setConnectTimeout(connectTimeout)
                .setTimeout(timeout)
                .setIdleConnectionTimeout(idleConnectionTimeout)
                .setUsername(redisUser)
                .setClientName(redisClientName)
                .setRetryAttempts(retryAttempts)
                .setRetryInterval(retryInterval)
                .setPassword(redisPwd);
        if (enableSsl) {
            singleConfig.setSslKeystore(Path.of(getString(REDIS_SSL_KEYSTORE_PATH)).toUri().toURL())
                    .setSslTruststore(Path.of(getString(REDIS_SSL_TRUSTSTORE_PATH)).toUri().toURL())
                    .setSslTruststorePassword(getPassword(REDIS_SSL_TRUSTSTORE_PASSWORD).value())
                    .setSslProtocols(getList(REDIS_SSL_PROVIDER).toArray(new String[0]))
            ;
        }
    }

    public RedissonClient getRedisson() {
        return redisson;
    }

    public RedisClientType getRedisClientType() {
        return redisClientType;
    }

    @NotNull
    private List<String> toNodes(List<String> redisNodes, String prefix) {
        return redisNodes.stream()
                .map(t -> toNode(t, prefix))
                .toList();
    }

    @NotNull
    private String toNode(String redisNode, String prefix) {
        String[] split = redisNode.split(StrPool.COLON);
        String host = split[0].strip();
        int port = Integer.parseInt(split[1].strip());
        return toNode(host, port, prefix);
    }

    @NotNull
    private String toNode(String host, int port, String prefix) {
        return prefix + host + CharPool.COLON + port;
    }

    private String toNode(InetSocketAddress addr, String prefix) {
        return prefix + addr.getHostName() + CharPool.COLON + addr.getPort();
    }

    @NotNull
    private <T extends BaseRedisNodes> List<RedisNode> getNodes(Config c, RedisNodes<T> nodes) {
        RedissonClient cli = Redisson.create(c);
        if (nodes.getClazz() == RedisSingle.class) {
            RedisSingle single = cli.getRedisNodes(RedisNodes.SINGLE);
            return List.of(single.getInstance());
        }
        if (nodes.getClazz() == RedisCluster.class) {
            RedisCluster redisCluster = cli.getRedisNodes(RedisNodes.CLUSTER);
            List<RedisNode> allNode = new ArrayList<>(16);
            allNode.addAll(redisCluster.getMasters());
            allNode.addAll(redisCluster.getSlaves());
            return allNode;
        }
        if (nodes.getClazz() == RedisSentinelMasterSlave.class) {
            RedisSentinelMasterSlave sentinelMasterSlave = cli.getRedisNodes(RedisNodes.SENTINEL_MASTER_SLAVE);
            List<RedisNode> allNode = new ArrayList<>(16);
            allNode.addAll(sentinelMasterSlave.getSentinels());
            return allNode;
        }
        if (nodes.getClazz() == RedisMasterSlave.class) {
            RedisMasterSlave masterSlave = cli.getRedisNodes(RedisNodes.MASTER_SLAVE);
            List<RedisNode> allNode = new ArrayList<>(16);
            allNode.add(masterSlave.getMaster());
            allNode.addAll(masterSlave.getSlaves());
            return allNode;
        }

        return List.of();
    }
}
