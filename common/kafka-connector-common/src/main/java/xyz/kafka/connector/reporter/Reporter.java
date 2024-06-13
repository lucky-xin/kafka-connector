package xyz.kafka.connector.reporter;

import xyz.kafka.connector.formatter.api.Formatter;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reporter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-01-04
 */
public class Reporter implements AutoCloseable {
    public static final String OFFSET_HEADER_KEY = "input_record_offset";
    public static final String TIMESTAMP_HEADER_KEY = "input_record_timestamp";
    public static final String PARTITION_HEADER_KEY = "input_record_partition";
    public static final String TOPIC_HEADER_KEY = "input_record_topic";
    private ReporterConfig reporterConfig;
    private ReporterAdminClient reporterAdminClient;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private final AtomicBoolean isOpen = new AtomicBoolean();
    private final SimpleHeaderConverter simpleHeaderConverter = new SimpleHeaderConverter();

    public void configure(ReporterConfig config) {
        this.reporterConfig = config;
        if (this.reporterConfig.isEnabled()) {
            this.kafkaProducer = createProducer(this.reporterConfig.producerProperties());
            this.reporterAdminClient = createReporterAdminClient(this.reporterConfig.adminProperties());
            createDestinationTopicsIfNeeded();
        }
        this.isOpen.set(true);
    }

    public Future<RecordMetadata> reportResult(SinkRecord original, Schema valueSchema, Object value) {
        return reportResult(original, null, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportResult(SinkRecord original, Map<String, String> userHeaders, Schema valueSchema, Object value) {
        return reportResult(original, userHeaders, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportResult(SinkRecord original, Map<String, String> userHeaders, Schema keySchema, Object key, Schema valueSchema, Object value) {
        if (this.reporterConfig.resultReportingEnabled()) {
            return reportMessage(original, userHeaders, this.reporterConfig.resultTopic(), keySchema, key, valueSchema, value, this.reporterConfig.resultKeyConverter(), this.reporterConfig.resultValueConverter());
        }
        return CompletableFuture.completedFuture(null);
    }

    public Future<RecordMetadata> reportError(SinkRecord original, Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return reportError(original, Schema.STRING_SCHEMA, sw.toString());
    }

    public Future<RecordMetadata> reportError(SinkRecord original, Schema valueSchema, Object value) {
        return reportError(original, null, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportError(SinkRecord original, Map<String, String> userHeaders, Schema valueSchema, Object value) {
        return reportError(original, userHeaders, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportError(SinkRecord original, Map<String, String> userHeaders, Schema keySchema, Object key, Schema valueSchema, Object value) {
        if (this.reporterConfig.errorReportingEnabled()) {
            return reportMessage(original, userHeaders, this.reporterConfig.errorTopic(), keySchema, key, valueSchema, value, this.reporterConfig.errorKeyConverter(), this.reporterConfig.errorValueConverter());
        }
        return CompletableFuture.completedFuture(null);
    }

    public void throwIfReporterClosed() {
        if (!this.isOpen.get()) {
            throw new ConnectException("Reporter already closed!");
        }
    }

    @Override
    public void close() {
        if (this.isOpen.compareAndSet(true, false)) {
            this.reporterConfig = null;
            this.reporterAdminClient = null;
            this.kafkaProducer.close();
            this.kafkaProducer = null;
        }
    }

    protected ReporterAdminClient createReporterAdminClient(Map<String, Object> adminProps) {
        return new ReporterAdminClient(adminProps);
    }

    protected KafkaProducer<byte[], byte[]> createProducer(Map<String, Object> producerProps) {
        producerProps.put("key.serializer", ByteArraySerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    protected void populateReportHeaders(Headers headers, SinkRecord original, Map<String, String> userHeaders, String targetTopic) {
        Objects.requireNonNull(original);
        headers.add(OFFSET_HEADER_KEY, this.simpleHeaderConverter.fromConnectHeader(targetTopic, OFFSET_HEADER_KEY, Schema.INT32_SCHEMA, original.kafkaOffset()));
        headers.add(TIMESTAMP_HEADER_KEY, this.simpleHeaderConverter.fromConnectHeader(targetTopic, TIMESTAMP_HEADER_KEY, Schema.INT64_SCHEMA, original.timestamp()));
        headers.add(PARTITION_HEADER_KEY, this.simpleHeaderConverter.fromConnectHeader(targetTopic, PARTITION_HEADER_KEY, Schema.INT32_SCHEMA, original.kafkaPartition()));
        headers.add(TOPIC_HEADER_KEY, this.simpleHeaderConverter.fromConnectHeader(targetTopic, TOPIC_HEADER_KEY, Schema.STRING_SCHEMA, original.topic()));
        if (userHeaders != null) {
            userHeaders.forEach((k, v) -> headers.add(targetTopic,
                    this.simpleHeaderConverter.fromConnectHeader(targetTopic, "", Schema.STRING_SCHEMA, v)));
        }
    }

    private void createDestinationTopicsIfNeeded() {
        if (this.reporterConfig.resultReportingEnabled()) {
            NewTopic successTopic = new NewTopic(this.reporterConfig.resultTopic(), this.reporterConfig.resultTopicPartitionCount(), this.reporterConfig.resultTopicReplicationFactor());
            successTopic.configs(this.reporterConfig.resultTopicProperties());
            this.reporterAdminClient.createTopic(successTopic);
        }
        if (this.reporterConfig.errorReportingEnabled()) {
            NewTopic successTopic2 = new NewTopic(this.reporterConfig.errorTopic(), this.reporterConfig.errorTopicPartitionCount(), this.reporterConfig.errorTopicReplicationFactor());
            successTopic2.configs(this.reporterConfig.errorTopicProperties());
            this.reporterAdminClient.createTopic(successTopic2);
        }
    }

    private Future<RecordMetadata> reportMessage(
            SinkRecord original,
            Map<String, String> userHeaders,
            String topic,
            Schema keySchema,
            Object key,
            Schema valueSchema,
            Object value,
            Formatter keyFormatter,
            Formatter valueFormatter) {
        throwIfReporterClosed();
        Headers headers = new RecordHeaders();
        populateReportHeaders(headers, original, userHeaders, topic);
        return this.kafkaProducer.send(new ProducerRecord<>(topic, null, keyFormatter.formatKey(topic, keySchema, key), valueFormatter.formatValue(topic, valueSchema, value), headers));
    }
}
