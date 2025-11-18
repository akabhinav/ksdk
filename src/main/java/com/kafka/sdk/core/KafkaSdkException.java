package com.kafka.sdk.core;

/**
 * Base exception for all Kafka SDK exceptions.
 */
public class KafkaSdkException extends RuntimeException {

    private final ErrorCode errorCode;
    private final boolean retriable;

    public KafkaSdkException(String message) {
        super(message);
        this.errorCode = ErrorCode.UNKNOWN;
        this.retriable = false;
    }

    public KafkaSdkException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCode.UNKNOWN;
        this.retriable = false;
    }

    public KafkaSdkException(String message, ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
        this.retriable = errorCode.isRetriable();
    }

    public KafkaSdkException(String message, Throwable cause, ErrorCode errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retriable = errorCode.isRetriable();
    }

    public KafkaSdkException(String message, ErrorCode errorCode, boolean retriable) {
        super(message);
        this.errorCode = errorCode;
        this.retriable = retriable;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public enum ErrorCode {
        // Configuration errors
        INVALID_CONFIGURATION("Invalid configuration", false),
        MISSING_CONFIGURATION("Missing required configuration", false),

        // Connection errors
        CONNECTION_FAILED("Failed to connect to Kafka", true),
        CONNECTION_TIMEOUT("Connection timeout", true),
        AUTHENTICATION_FAILED("Authentication failed", false),

        // Producer errors
        PRODUCER_SEND_FAILED("Failed to send message", true),
        PRODUCER_TIMEOUT("Producer timeout", true),
        SERIALIZATION_FAILED("Serialization failed", false),
        PRODUCER_CLOSED("Producer is closed", false),

        // Consumer errors
        CONSUMER_POLL_FAILED("Failed to poll messages", true),
        CONSUMER_COMMIT_FAILED("Failed to commit offsets", true),
        DESERIALIZATION_FAILED("Deserialization failed", false),
        CONSUMER_CLOSED("Consumer is closed", false),
        REBALANCE_FAILED("Rebalance failed", true),

        // Transaction errors
        TRANSACTION_FAILED("Transaction failed", true),
        TRANSACTION_TIMEOUT("Transaction timeout", true),
        TRANSACTION_ABORTED("Transaction aborted", false),

        // Admin errors
        TOPIC_CREATION_FAILED("Failed to create topic", true),
        TOPIC_DELETION_FAILED("Failed to delete topic", true),
        ADMIN_OPERATION_FAILED("Admin operation failed", true),

        // Stream errors
        STREAM_PROCESSING_FAILED("Stream processing failed", true),
        STATE_STORE_ERROR("State store error", true),

        // Circuit breaker
        CIRCUIT_BREAKER_OPEN("Circuit breaker is open", false),

        // Rate limiting
        RATE_LIMIT_EXCEEDED("Rate limit exceeded", true),

        // DLQ errors
        DLQ_PUBLISH_FAILED("Failed to publish to DLQ", true),

        // General errors
        UNKNOWN("Unknown error", false),
        INTERNAL_ERROR("Internal error", false);

        private final String description;
        private final boolean retriable;

        ErrorCode(String description, boolean retriable) {
            this.description = description;
            this.retriable = retriable;
        }

        public String getDescription() {
            return description;
        }

        public boolean isRetriable() {
            return retriable;
        }
    }
}
