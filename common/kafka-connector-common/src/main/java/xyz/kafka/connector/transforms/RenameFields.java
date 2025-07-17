package xyz.kafka.connector.transforms;

import cn.hutool.core.collection.CollUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 修改某些kafka主题字段记录字段名称
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public abstract class RenameFields<R extends ConnectRecord<R>> extends AbstractTransformation<R> {

    public static final String RENAME_PREFIX = "renames.";

    private Map<String, Map<String, String>> pairs;

    protected RenameFields() {
        super(new ConfigDef());
    }

    @Override
    protected void configure(Map<String, ?> configs, AbstractConfig config) {
        this.pairs = configs.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(RENAME_PREFIX))
                .map(e -> {
                    String key = e.getKey();
                    String topic = key.substring(RENAME_PREFIX.length());
                    String value = (String) e.getValue();
                    Map<String, String> mapper = Stream.of(value.split(","))
                            .map(s -> s.split(":"))
                            .collect(Collectors.toMap(
                                    s -> s[0],
                                    s -> s[1],
                                    (s1, s2) -> s1
                            ));
                    return new AbstractMap.SimpleEntry<>(topic, mapper);
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public R apply(R r) {
        Map<String, String> mapper = this.pairs.get(r.topic());
        if (CollUtil.isEmpty(mapper)) {
            return r;
        }
        Struct struct = Requirements.requireStructOrNull(value(r), r.topic());
        if (struct == null) {
            return r;
        }
        Schema schema = schema(r);
        Schema newSchema;
        if (isSchemaCache) {
            newSchema = schemaCache.get(r.topic(), k -> makeUpdatedSchema(k, schema));
        } else {
            newSchema = makeUpdatedSchema(r.topic(), schema);
        }
        Struct newValue = new Struct(newSchema);
        for (Field field : schema.fields()) {
            String newName = pairs.get(r.topic()).get(field.name());
            if (newName != null) {
                newValue.put(newName, struct.get(field));
                continue;
            }
            newValue.put(field.name(), struct.get(field));
        }
        return newRecord(r, newValue, newSchema);
    }

    private Schema makeUpdatedSchema(String topic, Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            String newName = pairs.get(topic).get(field.name());
            if (newName != null) {
                builder.field(newName, field.schema());
                continue;
            }
            builder.field(field.name(), field.schema());
        }
        return builder.build();
    }

    public static class Key<T extends ConnectRecord<T>> extends RenameFields<T>
            implements KeyOrValueTransformation.Key<T> {
    }

    public static class Value<T extends ConnectRecord<T>> extends RenameFields<T>
            implements KeyOrValueTransformation.Value<T> {
    }
}
