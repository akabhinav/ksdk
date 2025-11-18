package com.kafka.sdk.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Interceptor for producer messages.
 * Allows for message enrichment and transformation before sending.
 */
public interface ProducerInterceptor<K, V> {

    /**
     * Called before the message is sent.
     * Can be used to enrich or transform the message.
     *
     * @param record The original producer record
     * @return The transformed producer record
     */
    ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);

    /**
     * Called after a message has been acknowledged.
     *
     * @param metadata The record metadata
     * @param exception The exception if send failed, null otherwise
     */
    default void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // Default no-op
    }

    /**
     * Called when the interceptor is closed.
     */
    default void close() {
        // Default no-op
    }
}
