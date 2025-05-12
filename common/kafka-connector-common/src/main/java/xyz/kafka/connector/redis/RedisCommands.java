package xyz.kafka.connector.redis;

import cn.hutool.core.lang.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.GeoEntry;
import xyz.kafka.serialization.json.JsonData;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * RedisCommand
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@Getter
@AllArgsConstructor
public class RedisCommands {

    static ObjectMapper objectMapper = Jackson.newObjectMapper();

    public static Schema genSchema() {
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.name("redis.command")
                .doc("redis命令schema");
        for (RedisCommand cmd : RedisCommand.values()) {
            builder.field(cmd.name(), cmd.getSchema());
        }
        return builder.build();
    }

    public static JsonSchema toJsonSchema(Schema schema, JsonData jsonData) {
        JsonSchema jsonSchema = jsonData.fromConnectSchema(schema);
        return new JsonSchema(sorted(jsonSchema.toJsonNode()));
    }

    @NotNull
    private static JsonNode sorted(JsonNode jsonNode) {
        JsonNode properties = jsonNode.get("properties");
        if (jsonNode instanceof ObjectNode objectNode && properties != null) {
            Map<Integer, Pair<String, JsonNode>> m = new HashMap<>(objectNode.size());
            Iterator<Map.Entry<String, JsonNode>> itr = properties.fields();
            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> e = itr.next();
                String field = e.getKey();
                JsonNode node = e.getValue();
                if (node.get(JsonData.CONNECT_INDEX_PROP) instanceof IntNode in) {
                    m.put(in.intValue(), Pair.of(field, node));
                    continue;
                }
                throw new UnsupportedOperationException("not found " + JsonData.CONNECT_INDEX_PROP);
            }
            ObjectNode on = objectMapper.createObjectNode();
            for (Pair<String, JsonNode> pair : m.values()) {
                on.putIfAbsent(pair.getKey(), sorted(pair.getValue()));
            }
            objectNode.replace("properties", on);
            return objectNode;
        }
        return jsonNode;
    }

    public static Struct set(
            String key,
            String value,
            RedisCommand.Expiration expire,
            RedisCommand.Condition condition) {
        Struct struct = new Struct(RedisCommand.SET.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUE, value);
        if (expire != null) {
            struct.put(RedisConstants.EXPIRE_TYPE, expire.type.name())
                    .put(RedisConstants.TIME, expire.time);
        }
        if (condition != null) {
            struct.put(RedisConstants.CONDITION, condition.name());
        }
        return struct;
    }

    public static Struct del(String key) {
        return new Struct(RedisCommand.DEL.getSchema())
                .put(RedisConstants.KEY, key);
    }

    public static Struct setnx(String key, String value) {
        return new Struct(RedisCommand.SETNX.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUE, value);
    }

    public static Struct incr(String key) {
        return new Struct(RedisCommand.INCR.getSchema())
                .put(RedisConstants.KEY, key);
    }

    public static Struct incrby(String key, Long increment) {
        return new Struct(RedisCommand.INCRBY.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.INCREMENT, increment);
    }

    public static Struct incrbyfloat(String key, Double increment) {
        return new Struct(RedisCommand.INCRBYFLOAT.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.INCREMENT, increment);
    }

    public static Struct decr(String key) {
        return new Struct(RedisCommand.DECR.getSchema())
                .put(RedisConstants.KEY, key);
    }

    public static Struct decrby(String key, Long decrement) {
        return new Struct(RedisCommand.DECRBY.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.DECREMENT, decrement);
    }

    public static Struct persist(String key) {
        return new Struct(RedisCommand.PERSIST.getSchema())
                .put(RedisConstants.KEY, key);
    }

    public static Struct expire(String key, Long seconds) {
        return new Struct(RedisCommand.EXPIRE.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.SECONDS, seconds);
    }

    public static Struct expireat(String key, Long timestamp) {
        return new Struct(RedisCommand.EXPIREAT.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.TIMESTAMP, timestamp);
    }

    public static Struct pttl(String key) {
        return new Struct(RedisCommand.PTTL.getSchema())
                .put(RedisConstants.KEY, key);
    }

    public static Struct hset(String key, String field, String value) {
        return new Struct(RedisCommand.HSET.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.FIELD, field)
                .put(RedisConstants.VALUE, value);
    }

    public static Struct hsetnx(String key, String field, String value) {
        return new Struct(RedisCommand.HSETNX.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.FIELD, field)
                .put(RedisConstants.VALUE, value);
    }

    public static Struct hmset(String key, Map<String, String> hash) {
        return new Struct(RedisCommand.HMSET.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.FIELDS, hash);
    }

    public static Struct hdel(String key, List<String> fields) {
        return new Struct(RedisCommand.HDEL.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.FIELDS, fields);
    }

    public static Struct hincrby(String key, String field, Long increment) {
        return new Struct(RedisCommand.HINCRBY.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.FIELD, field)
                .put(RedisConstants.INCREMENT, increment);
    }

    public static Struct hincrbyfloat(String key, String field, Double increment) {
        return new Struct(RedisCommand.HINCRBYFLOAT.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.FIELD, field)
                .put(RedisConstants.INCREMENT, increment);
    }

    public static Struct lrem(String key, String field, Long count) {
        return new Struct(RedisCommand.LREM.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUE, field)
                .put(RedisConstants.COUNT, count);
    }

    public static Struct lpush(String key, List<String> values) {
        return new Struct(RedisCommand.LPUSH.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUES, values);
    }

    public static Struct lpop(String key, Integer count) {
        return new Struct(RedisCommand.LPOP.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.COUNT, count);
    }

    public static Struct rpush(String key, List<String> values) {
        return new Struct(RedisCommand.RPUSH.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUES, values);
    }

    public static Struct rpop(String key, Integer count) {
        return new Struct(RedisCommand.RPOP.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.COUNT, count);
    }

    public static Struct sadd(String key, List<String> values) {
        return new Struct(RedisCommand.SADD.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUES, values);
    }

    public static Struct srem(String key, List<String> values) {
        return new Struct(RedisCommand.SREM.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUES, values);
    }

    public static Struct zadd(String key, String member, Double score) {
        return new Struct(RedisCommand.ZADD.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.MEMBER, member)
                .put(RedisConstants.SCORE, score);
    }

    public static Struct zincrby(String key, String member, Double increment) {
        return new Struct(RedisCommand.ZINCRBY.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.MEMBER, member)
                .put(RedisConstants.INCREMENT, increment);
    }

    public static Struct zrem(String key, List<String> members) {
        return new Struct(RedisCommand.ZREM.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.MEMBERS, members);
    }

    public static Struct zremRangeByIndex(String key, Integer min, Integer max) {
        return new Struct(RedisCommand.ZREM_RANGE_BY_INDEX.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.MIN, min)
                .put(RedisConstants.MAX, max);
    }

    public static Struct zremRangeByScore(String key, Double start, Double end) {
        return new Struct(RedisCommand.ZREM_RANGE_BY_SCORE.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.START, start)
                .put(RedisConstants.END, end);
    }

    public static Struct geoAdd(String key, Map<String, GeoEntry> geos) {
        Field field = RedisCommand.GEO_ADD.getSchema().field(RedisConstants.VALUES);
        List<Struct> structs = geos.entrySet()
                .stream()
                .map(t -> new Struct(field.schema().valueSchema())
                        .put(RedisConstants.MEMBER, t.getKey())
                        .put(RedisConstants.LONGITUDE, t.getValue().getLongitude())
                        .put(RedisConstants.LATITUDE, t.getValue().getLatitude())
                ).toList();
        return new Struct(RedisCommand.GEO_ADD.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.VALUES, structs);
    }

    public static Struct jsonSet(String key, String path, String value) {
        return new Struct(RedisCommand.JSON_SET.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.PATH, path)
                .put(RedisConstants.VALUE, value);
    }

    public static Struct jsonArrAppend(String key, String path, String value) {
        return new Struct(RedisCommand.JSON_ARR_APPEND.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.PATH, path)
                .put(RedisConstants.VALUE, value);
    }

    public static Struct jsonArrInsert(String key, String path, Integer index, List<String> values) {
        return new Struct(RedisCommand.JSON_ARR_INSERT.getSchema())
                .put(RedisConstants.KEY, key)
                .put(RedisConstants.PATH, path)
                .put(RedisConstants.INDEX, index)
                .put(RedisConstants.VALUES, values);
    }
}
