package com.kafka.sdk.core;

/**
 * Exception for producer-related errors.
 */
public class ProducerException extends KafkaSdkException {

    private final String topic;
    private final Integer partition;
    private final Object key;

    public ProducerException(String message) {
        super(message, ErrorCode.PRODUCER_SEND_FAILED);
        this.topic = null;
        this.partition = null;
        this.key = null;
    }

    public ProducerException(String message, Throwable cause) {
        super(message, cause, ErrorCode.PRODUCER_SEND_FAILED);
        this.topic = null;
        this.partition = null;
        this.key = null;
    }

    public ProducerException(String message, String topic, Integer partition, Object key) {
        super(message, ErrorCode.PRODUCER_SEND_FAILED);
        this.topic = topic;
        this.partition = partition;
        this.key = key;
    }

    public ProducerException(String message, Throwable cause, String topic, Integer partition, Object key) {
        super(message, cause, ErrorCode.PRODUCER_SEND_FAILED);
        this.topic = topic;
        this.partition = partition;
        this.key = key;
    }

    public ProducerException(String message, ErrorCode errorCode, String topic) {
        super(message, errorCode);
        this.topic = topic;
        this.partition = null;
        this.key = null;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public Object getKey() {
        return key;
    }
}
