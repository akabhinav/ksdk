package com.kafka.sdk.util;

import org.testcontainers.kafka.KafkaContainer;

/**
 * Utility class for embedded Kafka for testing.
 */
public class EmbeddedKafka {

    private static KafkaContainer kafkaContainer;

    /**
     * Start an embedded Kafka instance.
     */
    public static String start() {
        if (kafkaContainer == null || !kafkaContainer.isRunning()) {
            kafkaContainer = new KafkaContainer("apache/kafka-native:3.8.0");
            kafkaContainer.start();
        }
        return kafkaContainer.getBootstrapServers();
    }

    /**
     * Stop the embedded Kafka instance.
     */
    public static void stop() {
        if (kafkaContainer != null && kafkaContainer.isRunning()) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }

    /**
     * Get the bootstrap servers.
     */
    public static String getBootstrapServers() {
        if (kafkaContainer == null || !kafkaContainer.isRunning()) {
            throw new IllegalStateException("Kafka is not running. Call start() first.");
        }
        return kafkaContainer.getBootstrapServers();
    }

    /**
     * Check if Kafka is running.
     */
    public static boolean isRunning() {
        return kafkaContainer != null && kafkaContainer.isRunning();
    }
}
