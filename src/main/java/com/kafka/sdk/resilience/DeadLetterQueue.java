package com.kafka.sdk.resilience;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.producer.ProducerFactory;
import com.kafka.sdk.producer.SmartProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Dead Letter Queue implementation for failed message handling.
 */
public class DeadLetterQueue<K, V> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueue.class);

    private final SmartProducer<K, V> producer;
    private final String dlqTopicSuffix;
    private final boolean includeOriginalHeaders;

    public DeadLetterQueue(KafkaSdkConfig config) {
        this(config, ".dlq", true);
    }

    public DeadLetterQueue(KafkaSdkConfig config, String dlqTopicSuffix, boolean includeOriginalHeaders) {
        this.producer = ProducerFactory.createStringProducer(config)
            .getClass()
            .cast(ProducerFactory.createStringProducer(config));
        this.dlqTopicSuffix = dlqTopicSuffix;
        this.includeOriginalHeaders = includeOriginalHeaders;
    }

    @SuppressWarnings("unchecked")
    public DeadLetterQueue(SmartProducer<K, V> producer, String dlqTopicSuffix, boolean includeOriginalHeaders) {
        this.producer = producer;
        this.dlqTopicSuffix = dlqTopicSuffix;
        this.includeOriginalHeaders = includeOriginalHeaders;
    }

    /**
     * Send a failed record to the DLQ.
     */
    public CompletableFuture<Void> send(ConsumerRecord<K, V> record, Exception exception) {
        String dlqTopic = record.topic() + dlqTopicSuffix;

        RecordHeaders headers = new RecordHeaders();

        // Copy original headers if configured
        if (includeOriginalHeaders && record.headers() != null) {
            record.headers().forEach(header -> headers.add(header.key(), header.value()));
        }

        // Add DLQ metadata
        headers.add("x-dlq-original-topic", record.topic().getBytes());
        headers.add("x-dlq-original-partition", String.valueOf(record.partition()).getBytes());
        headers.add("x-dlq-original-offset", String.valueOf(record.offset()).getBytes());
        headers.add("x-dlq-original-timestamp", String.valueOf(record.timestamp()).getBytes());
        headers.add("x-dlq-timestamp", String.valueOf(Instant.now().toEpochMilli()).getBytes());

        if (exception != null) {
            headers.add("x-dlq-exception-class", exception.getClass().getName().getBytes());
            headers.add("x-dlq-exception-message",
                (exception.getMessage() != null ? exception.getMessage() : "").getBytes());
        }

        return producer.send(dlqTopic, record.key(), record.value(), headers)
            .thenAccept(metadata -> {
                logger.info("Message sent to DLQ topic {} partition {} offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
            })
            .exceptionally(e -> {
                logger.error("Failed to send message to DLQ", e);
                return null;
            });
    }

    /**
     * Send a value to a specific DLQ topic.
     */
    public CompletableFuture<Void> send(String dlqTopic, K key, V value, Exception exception) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("x-dlq-timestamp", String.valueOf(Instant.now().toEpochMilli()).getBytes());

        if (exception != null) {
            headers.add("x-dlq-exception-class", exception.getClass().getName().getBytes());
            headers.add("x-dlq-exception-message",
                (exception.getMessage() != null ? exception.getMessage() : "").getBytes());
        }

        return producer.send(dlqTopic, key, value, headers)
            .thenAccept(metadata -> {
                logger.info("Message sent to DLQ topic {} partition {} offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
            })
            .exceptionally(e -> {
                logger.error("Failed to send message to DLQ", e);
                return null;
            });
    }

    @Override
    public void close() {
        producer.close();
    }
}
