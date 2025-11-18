package com.kafka.sdk.consumer;

import java.util.Map;

/**
 * Metrics interface for consumer statistics.
 */
public interface ConsumerMetrics {

    /**
     * Get total number of messages consumed.
     */
    long getMessagesConsumed();

    /**
     * Get number of successfully processed messages.
     */
    long getMessagesProcessed();

    /**
     * Get number of failed messages.
     */
    long getMessagesFailed();

    /**
     * Get processing rate (0.0 to 1.0).
     */
    double getProcessingRate();

    /**
     * Get raw Kafka consumer metrics.
     */
    Map<String, Object> getKafkaMetrics();
}
