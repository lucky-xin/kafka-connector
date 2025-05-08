package xyz.kafka.connector.reporter;

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
import xyz.kafka.connector.formatter.api.Formatter;

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
    private ReporterConfig conf;
    private ReporterAdminClient reporterAdminClient;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private final AtomicBoolean isOpen = new AtomicBoolean();
    private final SimpleHeaderConverter converter = new SimpleHeaderConverter();

    public void configure(ReporterConfig config) {
        this.conf = config;
        if (this.conf.isEnabled()) {
            this.kafkaProducer = createProducer(this.conf.producerProperties());
            this.reporterAdminClient = createReporterAdminClient(this.conf.adminProperties());
            createDestinationTopicsIfNeeded();
        }
        this.isOpen.set(true);
    }

    public Future<RecordMetadata> reportResult(SinkRecord original,
                                               Schema valueSchema,
                                               Object value) {
        return reportResult(original, null, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportResult(SinkRecord original,
                                               Map<String, String> userHeaders,
                                               Schema valueSchema,
                                               Object value) {
        return reportResult(original, userHeaders, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportResult(SinkRecord original,
                                               Map<String, String> userHeaders,
                                               Schema keySchema,
                                               Object key,
                                               Schema valueSchema,
                                               Object value) {
        if (this.conf.resultReportingEnabled()) {
            return reportMessage(original, userHeaders, this.conf.resultTopic(), keySchema, key, valueSchema, value,
                    this.conf.resultKeyConverter(), this.conf.resultValueConverter());
        }
        return CompletableFuture.completedFuture(null);
    }

    public Future<RecordMetadata> reportError(SinkRecord original,
                                              Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return reportError(original, Schema.STRING_SCHEMA, sw.toString());
    }

    public Future<RecordMetadata> reportError(SinkRecord original,
                                              Schema valueSchema,
                                              Object value) {
        return reportError(original, null, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportError(SinkRecord original,
                                              Map<String, String> userHeaders,
                                              Schema valueSchema,
                                              Object value) {
        return reportError(original, userHeaders, original.keySchema(), original.key(), valueSchema, value);
    }

    public Future<RecordMetadata> reportError(SinkRecord original,
                                              Map<String, String> userHeaders,
                                              Schema keySchema,
                                              Object key,
                                              Schema valueSchema,
                                              Object value) {
        if (this.conf.errorReportingEnabled()) {
            return reportMessage(original, userHeaders, this.conf.errorTopic(), keySchema, key, valueSchema, value,
                    this.conf.errorKeyConverter(), this.conf.errorValueConverter());
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
            this.conf = null;
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

    protected void populateReportHeaders(Headers headers,
                                         SinkRecord original,
                                         Map<String, String> userHeaders,
                                         String topic) {
        Objects.requireNonNull(original);
        headers.add(OFFSET_HEADER_KEY,
                this.converter.fromConnectHeader(
                        topic,
                        OFFSET_HEADER_KEY,
                        Schema.INT32_SCHEMA,
                        original.kafkaOffset()
                )
        );
        headers.add(TIMESTAMP_HEADER_KEY,
                this.converter.fromConnectHeader(
                        topic,
                        TIMESTAMP_HEADER_KEY,
                        Schema.INT64_SCHEMA,
                        original.timestamp()
                )
        );
        headers.add(PARTITION_HEADER_KEY,
                this.converter.fromConnectHeader(
                        topic,
                        PARTITION_HEADER_KEY,
                        Schema.INT32_SCHEMA,
                        original.kafkaPartition()
                )
        );
        headers.add(TOPIC_HEADER_KEY,
                this.converter.fromConnectHeader(topic, TOPIC_HEADER_KEY, Schema.STRING_SCHEMA, original.topic()));
        if (userHeaders != null) {
            userHeaders.forEach((k, v) -> headers.add(topic,
                    this.converter.fromConnectHeader(topic, "", Schema.STRING_SCHEMA, v)));
        }
    }

    private void createDestinationTopicsIfNeeded() {
        if (this.conf.resultReportingEnabled()) {
            NewTopic successTopic = new NewTopic(
                    this.conf.resultTopic(),
                    this.conf.resultTopicPartitionCount(),
                    this.conf.resultTopicReplicationFactor()
            );
            successTopic.configs(this.conf.resultTopicProperties());
            this.reporterAdminClient.createTopic(successTopic);
        }
        if (this.conf.errorReportingEnabled()) {
            NewTopic successTopic2 = new NewTopic(
                    this.conf.errorTopic(),
                    this.conf.errorTopicPartitionCount(),
                    this.conf.errorTopicReplicationFactor()
            );
            successTopic2.configs(this.conf.errorTopicProperties());
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
        return this.kafkaProducer.send(new ProducerRecord<>(topic, null,
                keyFormatter.formatKey(topic, keySchema, key), valueFormatter.formatValue(topic, valueSchema, value), headers));
    }
}
