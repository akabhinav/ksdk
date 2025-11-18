package com.kafka.sdk.core;

/**
 * Exception for consumer-related errors.
 */
public class ConsumerException extends KafkaSdkException {

    private final String topic;
    private final Integer partition;
    private final Long offset;
    private final String groupId;

    public ConsumerException(String message) {
        super(message, ErrorCode.CONSUMER_POLL_FAILED);
        this.topic = null;
        this.partition = null;
        this.offset = null;
        this.groupId = null;
    }

    public ConsumerException(String message, Throwable cause) {
        super(message, cause, ErrorCode.CONSUMER_POLL_FAILED);
        this.topic = null;
        this.partition = null;
        this.offset = null;
        this.groupId = null;
    }

    public ConsumerException(String message, String topic, Integer partition, Long offset) {
        super(message, ErrorCode.CONSUMER_POLL_FAILED);
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.groupId = null;
    }

    public ConsumerException(String message, Throwable cause, String topic, Integer partition, Long offset) {
        super(message, cause, ErrorCode.CONSUMER_POLL_FAILED);
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.groupId = null;
    }

    public ConsumerException(String message, ErrorCode errorCode, String groupId) {
        super(message, errorCode);
        this.topic = null;
        this.partition = null;
        this.offset = null;
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public Long getOffset() {
        return offset;
    }

    public String getGroupId() {
        return groupId;
    }
}
