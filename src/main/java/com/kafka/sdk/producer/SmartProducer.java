package com.kafka.sdk.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Smart producer interface with enterprise-grade features.
 * Provides guaranteed delivery, batching, and advanced partitioning strategies.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface SmartProducer<K, V> extends AutoCloseable {

    /**
     * Send a message to a topic with automatic key extraction and partitioning.
     */
    CompletableFuture<RecordMetadata> send(String topic, V value);

    /**
     * Send a message with explicit key.
     */
    CompletableFuture<RecordMetadata> send(String topic, K key, V value);

    /**
     * Send a message with explicit partition.
     */
    CompletableFuture<RecordMetadata> send(String topic, Integer partition, K key, V value);

    /**
     * Send a message with headers.
     */
    CompletableFuture<RecordMetadata> send(String topic, K key, V value, Headers headers);

    /**
     * Send a message with timestamp.
     */
    CompletableFuture<RecordMetadata> send(String topic, Integer partition, Long timestamp, K key, V value);

    /**
     * Send a full producer record.
     */
    CompletableFuture<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * Send multiple messages as a batch.
     */
    List<CompletableFuture<RecordMetadata>> sendBatch(String topic, List<V> values);

    /**
     * Send multiple messages with keys as a batch.
     */
    List<CompletableFuture<RecordMetadata>> sendBatch(String topic, Map<K, V> keyValues);

    /**
     * Send a priority message that bypasses normal batching.
     */
    CompletableFuture<RecordMetadata> sendPriority(String topic, K key, V value, MessagePriority priority);

    /**
     * Begin a transaction (requires transactional.id configuration).
     */
    void beginTransaction();

    /**
     * Commit the current transaction.
     */
    void commitTransaction();

    /**
     * Abort the current transaction.
     */
    void abortTransaction();

    /**
     * Send a message within a transaction.
     */
    CompletableFuture<RecordMetadata> sendInTransaction(String topic, K key, V value);

    /**
     * Flush any batched messages.
     */
    void flush();

    /**
     * Get producer metrics.
     */
    ProducerMetrics getMetrics();

    /**
     * Check if the producer is healthy.
     */
    boolean isHealthy();

    /**
     * Add a message interceptor.
     */
    void addInterceptor(ProducerInterceptor<K, V> interceptor);

    /**
     * Remove a message interceptor.
     */
    void removeInterceptor(ProducerInterceptor<K, V> interceptor);

    /**
     * Close the producer.
     */
    @Override
    void close();

    /**
     * Message priority levels.
     */
    enum MessagePriority {
        LOW(0),
        NORMAL(1),
        HIGH(2),
        CRITICAL(3);

        private final int level;

        MessagePriority(int level) {
            this.level = level;
        }

        public int getLevel() {
            return level;
        }
    }
}
