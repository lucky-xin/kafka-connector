package xyz.kafka.connector.convert;

import xyz.kafka.connector.convert.protobuf.ProtobufConverter;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * SinkRecordConverter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-10-12
 */
public class SinkRecordConverter {

    public static final String HEADERS_FIELD = "headers";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String OFFSET_FIELD = "offset";
    public static final String TOPIC_FIELD = "topic";
    public static final String PARTITION_FIELD = "partition";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String TIMESTAMP_TYPE_FIELD = "timestampType";
    public static final String SINK_RECORD_SUBJECT = "sink_record_proto";
    private final Converter localCacheKeyConverter;
    private final Converter localCacheValueConverter;
    private final ProtobufConverter protobufConverter = new ProtobufConverter();
    private final SimpleHeaderConverter headerConverter = new SimpleHeaderConverter();

    private final Schema schema = SchemaBuilder.struct()
            .field(TOPIC_FIELD, Schema.STRING_SCHEMA)
            .field(PARTITION_FIELD, Schema.INT32_SCHEMA)
            .field(TIMESTAMP_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
            .field(OFFSET_FIELD, Schema.INT64_SCHEMA)
            .field(TIMESTAMP_TYPE_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
            .field(HEADERS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
            .field(KEY_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(VALUE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    public SinkRecordConverter(Converter localCacheKeyConverter,
                               Converter localCacheValueConverter
    ) {
        this.localCacheKeyConverter = localCacheKeyConverter;
        this.localCacheValueConverter = localCacheValueConverter;
        this.protobufConverter.configure(Map.of(
                "use.latest.version", "true",
                "auto.register.schemas", "false",
                "latest.compatibility.strict", "false"
        ), false);
    }

    public byte[] fromConnectData(SinkRecord sr) {
        Struct struct = new Struct(schema)
                .put(TOPIC_FIELD, sr.topic())
                .put(PARTITION_FIELD, sr.kafkaPartition())
                .put(TIMESTAMP_FIELD, sr.timestamp())
                .put(OFFSET_FIELD, sr.kafkaOffset());
        if (sr.timestampType() != null) {
            struct.put(TIMESTAMP_TYPE_FIELD, sr.timestampType().ordinal());
        }
        Headers all = sr.headers();
        Iterator<Header> itr = all.iterator();
        Map<String, Object> map = new HashMap<>(all.size());
        while (itr.hasNext()) {
            Header h = itr.next();
            byte[] bytes = this.headerConverter.fromConnectHeader(sr.topic(), h.key(), h.schema(), h.value());
            map.put(h.key(), new String(bytes, StandardCharsets.UTF_8));
        }
        struct.put(HEADERS_FIELD, map);
        byte[] keyBytes = localCacheKeyConverter.fromConnectData(sr.topic(), sr.keySchema(), sr.key());
        struct.put(KEY_FIELD, Base64.getEncoder().encodeToString(keyBytes));
        byte[] valueBytes = localCacheValueConverter.fromConnectData(sr.topic(), sr.valueSchema(), sr.value());
        struct.put(VALUE_FIELD, Base64.getEncoder().encodeToString(valueBytes));
        return protobufConverter.fromConnectData(SINK_RECORD_SUBJECT, struct.schema(), struct);
    }

    public SinkRecord toSinkRecord(byte[] bytes) {
        SchemaAndValue sav = protobufConverter.toConnectData(SINK_RECORD_SUBJECT, bytes);
        Struct struct = (Struct) sav.value();
        String topic = struct.getString(TOPIC_FIELD);
        Integer partition = struct.getInt32(PARTITION_FIELD);
        String key = struct.getString(KEY_FIELD);
        String value = struct.getString(VALUE_FIELD);
        long offset = struct.getInt64(OFFSET_FIELD);
        Long timestamp = struct.getInt64(TIMESTAMP_FIELD);
        TimestampType timestampType = null;
        Integer ordinal = struct.getInt32(TIMESTAMP_TYPE_FIELD);
        if (ordinal != null) {
            timestampType = TimestampType.values()[ordinal];
        }
        SchemaAndValue sk = convert(key, topic, true);
        SchemaAndValue sv = convert(value, topic, false);
        ConnectHeaders headers = null;
        Map<String, String> tmp = struct.getMap(HEADERS_FIELD);
        if (tmp != null) {
            headers = new ConnectHeaders();
            for (Map.Entry<String, String> e : tmp.entrySet()) {
                SchemaAndValue sav1 = headerConverter.toConnectHeader(topic, e.getKey(),
                        e.getValue().getBytes(StandardCharsets.UTF_8));
                headers.add(e.getKey(), sav1);
            }
        }
        return new SinkRecord(topic, partition, sk.schema(), sk.value(), sv.schema(),
                sv.value(), offset, timestamp, timestampType, headers);
    }

    @NotNull
    private SchemaAndValue convert(String content, String topic, boolean isKey) {
        if (content == null) {
            return new SchemaAndValue(null, null);
        }
        if (isKey) {
            return localCacheKeyConverter.toConnectData(topic, Base64.getDecoder().decode(content));
        }
        return localCacheValueConverter.toConnectData(topic, Base64.getDecoder().decode(content));
    }

}
