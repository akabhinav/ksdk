package com.kafka.sdk.producer;

import java.util.Map;

/**
 * Metrics interface for producer statistics.
 */
public interface ProducerMetrics {

    /**
     * Get total number of messages sent.
     */
    long getMessagesSent();

    /**
     * Get number of successfully delivered messages.
     */
    long getMessagesSucceeded();

    /**
     * Get number of failed messages.
     */
    long getMessagesFailed();

    /**
     * Get success rate (0.0 to 1.0).
     */
    double getSuccessRate();

    /**
     * Get raw Kafka producer metrics.
     */
    Map<String, Object> getKafkaMetrics();
}
