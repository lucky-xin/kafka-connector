package xyz.kafka.connector.lock;

import cn.hutool.core.util.IdUtil;
import lombok.Builder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * KafkaDistributedLock
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2025-02-06
 */
@Builder
public class KafkaDistributedLock implements AutoCloseable {
    private String topic;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    private final AtomicBoolean lockAcquired = new AtomicBoolean(false);

    public KafkaDistributedLock(String topic, Properties properties) {
        // 初始化 Kafka Producer
        Properties producerProps = new Properties(properties);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            if (!adminClient.listTopics().names().get().contains(topic)) {
                adminClient.createTopics(Collections.singleton(
                                new NewTopic(topic, 1, (short) 3)
                                        .configs(
                                                Map.of("retention.ms", Duration.ofDays(1).toString())
                                        )
                        )
                ).all().get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException(e);
        } catch (ExecutionException e) {
            throw new ConnectException(e);
        }
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // 初始化 Kafka Consumer
        Properties consumerProps = new Properties(properties);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lock-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public boolean tryLock(String lockKey, Duration timeout) {
        long startTime = System.currentTimeMillis();
        while (!lockAcquired.get() && (System.currentTimeMillis() - startTime < timeout.toMillis())) {
            // 发送锁消息
            String value = "lock_request" + IdUtil.nanoId() + ":" + IdUtil.fastUUID();
            producer.send(new ProducerRecord<>(topic, lockKey, value));
            // 消费消息，检查是否获取到锁
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> r : records) {
                if (r.key().equals(lockKey) && r.value().equals(value)) {
                    lockAcquired.set(true);
                    return true;
                }
                consumer.commitSync();
            }
        }
        return false;
    }

    public void unlock() {
        if (lockAcquired.compareAndSet(true, false)) {
            consumer.commitSync();
        }
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }
}
