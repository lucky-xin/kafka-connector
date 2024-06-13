package xyz.kafka.connector.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.redisson.api.GeoEntry;
import org.redisson.api.RBatch;
import org.redisson.api.RBucketAsync;
import org.redisson.api.RListAsync;
import org.redisson.codec.JacksonCodec;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * RedisCommand
 *<a href="https://github.com/redisson/redisson/wiki/11.-Redis-commands-mapping">...</a>
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@AllArgsConstructor
@Getter
public enum RedisCommand {
    /**
     * set value
     */
    SET(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String val = struct.getString(RedisConstants.VALUE);
                String expireType = struct.getString(RedisConstants.EXPIRE_TYPE);
                Long time = struct.getInt64(RedisConstants.TIME);
                String conditionString = struct.getString(RedisConstants.CONDITION);
                RBucketAsync<String> bucket = batch.getBucket(key);
                if (time != null) {
                    Expiration.Type type = Optional.ofNullable(expireType)
                            .map(Expiration.Type::valueOf)
                            .orElse(null);
                    Duration duration = null;
                    if (type != null) {
                        if (type == Expiration.Type.EX) {
                            duration = Duration.ofSeconds(time);
                        } else if (type == Expiration.Type.PX) {
                            duration = Duration.ofMillis(time);
                        }
                    }
                    if (conditionString != null) {
                        Condition condition = Condition.valueOf(conditionString);
                        if (condition == Condition.NX) {
                            if (duration != null) {
                                bucket.setIfAbsentAsync(val, duration);
                            } else {
                                bucket.setIfAbsentAsync(val);
                            }
                        } else if (condition == Condition.XX) {
                            if (duration != null) {
                                bucket.setIfExistsAsync(val, duration);
                            } else {
                                bucket.setIfExistsAsync(val);
                            }
                        }
                    }
                }

            },
            SchemaBuilder.struct()
                    .name("xyz.redis.set")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .field(RedisConstants.CONDITION, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(RedisConstants.EXPIRE_TYPE, Schema.STRING_SCHEMA)
                    .field(RedisConstants.TIME, Schema.INT64_SCHEMA)
                    .build()
    ),

    DEL(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                batch.getBucket(key).deleteAsync();
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.del")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .build()
    ),
    SETNX(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String val = struct.getString(RedisConstants.VALUE);
                batch.getBucket(key).setIfAbsentAsync(val);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.setnx")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .build()
    ),
    INCR(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                batch.getAtomicLong(key).incrementAndGetAsync();
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.incr")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .build()
    ),
    INCRBY(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Long increment = struct.getInt64(RedisConstants.INCREMENT);
                batch.getAtomicLong(key).addAndGetAsync(increment);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.incrby")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.INCREMENT, Schema.INT64_SCHEMA)
                    .build()
    ),
    INCRBYFLOAT(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Double increment = struct.getFloat64(RedisConstants.INCREMENT);
                batch.getAtomicDouble(key).addAndGetAsync(increment);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.incrbyfloat")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.INCREMENT, Schema.FLOAT64_SCHEMA)
                    .build()
    ),
    DECR(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                batch.getAtomicLong(key).decrementAndGetAsync();
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.decr")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .build()
    ),
    DECRBY(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Long decrement = struct.getInt64(RedisConstants.DECREMENT);
                batch.getAtomicLong(key).addAndGetAsync(decrement);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.decrby")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.DECREMENT, Schema.INT64_SCHEMA)
                    .build()
    ),

    PERSIST(

            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                batch.getKeys().clearExpireAsync(key);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.persist")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .build()
    ),
    EXPIRE(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Long seconds = struct.getInt64(RedisConstants.SECONDS);
                batch.getKeys().expireAsync(key, seconds, TimeUnit.SECONDS);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.expire")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.SECONDS, Schema.INT64_SCHEMA)
                    .build()
    ),
    EXPIREAT(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Long timestamp = struct.getInt64(RedisConstants.TIMESTAMP);
                batch.getKeys().expireAtAsync(key, timestamp);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.expireat")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.TIMESTAMP, Schema.INT64_SCHEMA)
                    .build()
    ),
    PTTL(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                batch.getKeys().remainTimeToLiveAsync(key);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.pttl")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .build()
    ),
    HSET(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String field = struct.getString(RedisConstants.FIELD);
                String value = struct.getString(RedisConstants.VALUE);
                batch.getMap(key).putAsync(field, value);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.hset")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.FIELD, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .build()
    ),
    HSETNX(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String field = struct.getString(RedisConstants.FIELD);
                String value = struct.getString(RedisConstants.VALUE);
                batch.getMap(key).fastPutIfAbsentAsync(field, value);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.hsetnx")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.FIELD, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .build()
    ),
    HMSET(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Map<String, String> hash = struct.getMap(RedisConstants.FIELDS);
                batch.getMap(key).putAllAsync(hash);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.hmset")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.FIELDS, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
                    .build()
    ),
    HDEL(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<String> fields = struct.getArray(RedisConstants.FIELDS);
                batch.getMap(key).fastRemoveAsync(fields.toArray(new String[0]));
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.hdel")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.FIELDS, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    ),

    HINCRBY(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String field = struct.getString(RedisConstants.FIELD);
                Long increment = struct.getInt64(RedisConstants.INCREMENT);
                batch.getMap(key).addAndGetAsync(field, increment);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.hincrby")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.FIELD, Schema.STRING_SCHEMA)
                    .field(RedisConstants.INCREMENT, Schema.INT64_SCHEMA)
                    .build()
    ),
    HINCRBYFLOAT(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String field = struct.getString(RedisConstants.FIELD);
                Double increment = struct.getFloat64(RedisConstants.INCREMENT);
                batch.getMap(key).addAndGetAsync(field, increment);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.hincrbyfloat")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.FIELD, Schema.STRING_SCHEMA)
                    .field(RedisConstants.INCREMENT, Schema.FLOAT64_SCHEMA)
                    .build()
    ),
    LREM(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String value = struct.getString(RedisConstants.VALUE);
                Long count = struct.getInt64(RedisConstants.COUNT);
                batch.getList(key).removeAsync(value, Math.toIntExact(count));
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.lrem")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .field(RedisConstants.COUNT, Schema.INT64_SCHEMA)
                    .build()
    ),
    LPUSH(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<String> values = struct.getArray(RedisConstants.VALUES);
                RListAsync<String> list = batch.getList(key);
                values.forEach(v -> list.addAsync(0, v));
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.lpush")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUES, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    ),
    LPOP(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Integer count = Optional.ofNullable(struct.getInt32(RedisConstants.COUNT)).orElse(1);
                batch.getQueue(key).pollAsync(count);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.lpop")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.COUNT, Schema.OPTIONAL_INT32_SCHEMA)
                    .build()
    ),
    RPUSH(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<String> values = struct.getArray(RedisConstants.VALUES);
                batch.getList(key).addAllAsync(values);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.rpush")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUES, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    ),
    RPOP(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Integer count = Optional.ofNullable(struct.getInt32(RedisConstants.COUNT)).orElse(1);
                batch.getDeque(key).pollAsync(count);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.rpop")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.COUNT, Schema.OPTIONAL_INT32_SCHEMA)
                    .build()
    ),
    SADD(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<String> values = struct.getArray(RedisConstants.VALUES);
                batch.getSet(key).addAllAsync(values);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.sadd")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUES, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    ),
    SREM(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<String> values = struct.getArray(RedisConstants.VALUES);
                batch.getSet(key).removeAllAsync(values);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.srem")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUES, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    ),
    ZADD(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String member = struct.getString(RedisConstants.MEMBER);
                Double score = struct.getFloat64(RedisConstants.SCORE);
                batch.getScoredSortedSet(key).addAsync(score, member);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.zadd")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.MEMBER, Schema.STRING_SCHEMA)
                    .field(RedisConstants.SCORE, Schema.FLOAT64_SCHEMA)
                    .build()
    ),
    ZINCRBY(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String member = struct.getString(RedisConstants.MEMBER);
                Double increment = struct.getFloat64(RedisConstants.INCREMENT);
                batch.getScoredSortedSet(key).intersectionAsync(Map.of(member, increment));
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.zincrby")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.MEMBER, Schema.STRING_SCHEMA)
                    .field(RedisConstants.INCREMENT, Schema.FLOAT64_SCHEMA)
                    .build()
    ),
    ZREM(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<String> members = struct.getArray(RedisConstants.MEMBERS);
                batch.getScoredSortedSet(key).removeAllAsync(members);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.zrem")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.MEMBERS, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    ),
    ZREM_RANGE_BY_INDEX(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                Integer min = struct.getInt32(RedisConstants.MIN);
                Integer max = struct.getInt32(RedisConstants.MAX);
                batch.getScoredSortedSet(key).removeRangeByRankAsync(min, max);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.zrem_range_by_lex")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.MIN, Schema.INT32_SCHEMA)
                    .field(RedisConstants.MAX, Schema.INT32_SCHEMA)
                    .build()
    ),
    ZREM_RANGE_BY_SCORE(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                double start = struct.getFloat64(RedisConstants.START);
                double end = struct.getFloat64(RedisConstants.END);
                batch.getScoredSortedSet(key).removeRangeByScoreAsync(start, true, end, true);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.zrem_range_by_score")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.START, Schema.FLOAT64_SCHEMA)
                    .field(RedisConstants.END, Schema.FLOAT64_SCHEMA)
                    .build()
    ),

    GEO_ADD(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                List<Object> values = struct.getArray(RedisConstants.VALUES);
                GeoEntry[] entries = values.stream()
                        .filter(Struct.class::isInstance)
                        .map(t -> (Struct) t)
                        .map(t ->
                                new GeoEntry(t.getFloat64(RedisConstants.LONGITUDE),
                                        t.getFloat64(RedisConstants.LATITUDE),
                                        t.getString(RedisConstants.MEMBER))
                        ).toArray(GeoEntry[]::new);
                batch.getGeo(key).addAsync(entries);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.geo_add")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.VALUES,
                            SchemaBuilder.array(
                                    SchemaBuilder.struct()
                                            .field(RedisConstants.MEMBER, Schema.STRING_SCHEMA)
                                            .field(RedisConstants.LONGITUDE, Schema.FLOAT64_SCHEMA)
                                            .field(RedisConstants.LATITUDE, Schema.FLOAT64_SCHEMA)
                                            .build()

                            ).build()
                    ).build()
    ),
    JSON_SET(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String path = struct.getString(RedisConstants.PATH);
                String value = struct.getString(RedisConstants.VALUE);
                char c = value.strip().charAt(0);
                if (c == '{') {
                    batch.getJsonBucket(key, new JacksonCodec<>(Map.class)).setAsync(path, value);
                } else {
                    batch.getJsonBucket(key, new JacksonCodec<>(List.class)).setAsync(path, value);
                }
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.json_set")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.PATH, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .build()
    ),
    JSON_ARR_APPEND(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String path = struct.getString(RedisConstants.PATH);
                String value = struct.getString(RedisConstants.VALUE);
                batch.getJsonBucket(key, new JacksonCodec<>(List.class)).setAsync(path, value);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.json_arr_append")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.PATH, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(RedisConstants.VALUE, Schema.STRING_SCHEMA)
                    .build()
    ),
    JSON_ARR_INSERT(
            (struct, batch) -> {
                String key = struct.getString(RedisConstants.KEY);
                String path = struct.getString(RedisConstants.PATH);
                Integer index = struct.getInt32(RedisConstants.INDEX);
                List<String> values = struct.getArray(RedisConstants.VALUES);
                Object[] objects = values.toArray();
                batch.getJsonBucket(key, new JacksonCodec<>(List.class))
                        .arrayAppendAsync(path, index, objects);
            },
            SchemaBuilder.struct()
                    .name("xyz.redis.json_arr_insert")
                    .field(RedisConstants.KEY, Schema.STRING_SCHEMA)
                    .field(RedisConstants.PATH, Schema.STRING_SCHEMA)
                    .field(RedisConstants.INDEX, Schema.INT32_SCHEMA)
                    .field(RedisConstants.VALUES, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .build()
    );

    private final BiConsumer<Struct, RBatch> exec;
    private final Schema schema;

    public static RedisCommand of(String cmd) {
        return Stream.of(values())
                .filter(t -> t.schema.name().equals(cmd))
                .findFirst()
                .orElse(null);
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Expiration {
        public enum Type {EX, PX}

        Type type = Type.EX;
        long time;
    }

    public enum Condition {
        /**
         * not exist
         */
        NX, XX
    }
}
