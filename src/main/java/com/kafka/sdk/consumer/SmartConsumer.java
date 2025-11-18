package com.kafka.sdk.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Smart consumer interface with enterprise-grade features.
 * Provides parallel processing, offset management, and error handling.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface SmartConsumer<K, V> extends AutoCloseable {

    /**
     * Subscribe to topics with a message handler.
     */
    void subscribe(Collection<String> topics, Consumer<ConsumerRecord<K, V>> handler);

    /**
     * Subscribe to a single topic.
     */
    void subscribe(String topic, Consumer<ConsumerRecord<K, V>> handler);

    /**
     * Subscribe to topics matching a pattern.
     */
    void subscribePattern(String pattern, Consumer<ConsumerRecord<K, V>> handler);

    /**
     * Assign specific partitions.
     */
    void assign(Collection<TopicPartition> partitions, Consumer<ConsumerRecord<K, V>> handler);

    /**
     * Start consuming messages.
     */
    void start();

    /**
     * Stop consuming messages.
     */
    void stop();

    /**
     * Pause consumption.
     */
    void pause();

    /**
     * Resume consumption.
     */
    void resume();

    /**
     * Commit offsets synchronously.
     */
    void commitSync();

    /**
     * Commit offsets asynchronously.
     */
    void commitAsync();

    /**
     * Commit specific offsets.
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * Seek to a specific offset.
     */
    void seek(TopicPartition partition, long offset);

    /**
     * Seek to beginning of partitions.
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * Seek to end of partitions.
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * Seek to a specific timestamp.
     */
    void seekToTimestamp(Collection<TopicPartition> partitions, long timestamp);

    /**
     * Get current partition assignment.
     */
    Set<TopicPartition> assignment();

    /**
     * Get consumer lag for all partitions.
     */
    Map<TopicPartition, Long> getLag();

    /**
     * Get consumer metrics.
     */
    ConsumerMetrics getMetrics();

    /**
     * Check if consumer is healthy.
     */
    boolean isHealthy();

    /**
     * Check if consumer is running.
     */
    boolean isRunning();

    /**
     * Add a rebalance listener.
     */
    void addRebalanceListener(RebalanceListener listener);

    /**
     * Remove a rebalance listener.
     */
    void removeRebalanceListener(RebalanceListener listener);

    /**
     * Set error handler for processing failures.
     */
    void setErrorHandler(ErrorHandler<K, V> errorHandler);

    /**
     * Add a message filter.
     */
    void addFilter(MessageFilter<K, V> filter);

    /**
     * Remove a message filter.
     */
    void removeFilter(MessageFilter<K, V> filter);

    /**
     * Get the group ID.
     */
    String getGroupId();

    /**
     * Close the consumer.
     */
    @Override
    void close();

    /**
     * Rebalance listener interface.
     */
    interface RebalanceListener {
        void onPartitionsRevoked(Collection<TopicPartition> partitions);
        void onPartitionsAssigned(Collection<TopicPartition> partitions);
    }

    /**
     * Error handler interface.
     */
    interface ErrorHandler<K, V> {
        ErrorAction handleError(ConsumerRecord<K, V> record, Exception exception);
    }

    /**
     * Actions to take on error.
     */
    enum ErrorAction {
        RETRY,
        SKIP,
        DEAD_LETTER_QUEUE,
        STOP
    }

    /**
     * Message filter interface.
     */
    interface MessageFilter<K, V> {
        boolean accept(ConsumerRecord<K, V> record);
    }
}
