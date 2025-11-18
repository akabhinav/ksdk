package com.kafka.sdk.core;

/**
 * Exception for serialization/deserialization errors.
 */
public class SerializationException extends KafkaSdkException {

    private final Class<?> targetType;
    private final byte[] data;

    public SerializationException(String message) {
        super(message, ErrorCode.SERIALIZATION_FAILED);
        this.targetType = null;
        this.data = null;
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause, ErrorCode.SERIALIZATION_FAILED);
        this.targetType = null;
        this.data = null;
    }

    public SerializationException(String message, Class<?> targetType) {
        super(message, ErrorCode.SERIALIZATION_FAILED);
        this.targetType = targetType;
        this.data = null;
    }

    public SerializationException(String message, Throwable cause, Class<?> targetType) {
        super(message, cause, ErrorCode.SERIALIZATION_FAILED);
        this.targetType = targetType;
        this.data = null;
    }

    public SerializationException(String message, Throwable cause, Class<?> targetType, byte[] data) {
        super(message, cause, ErrorCode.DESERIALIZATION_FAILED);
        this.targetType = targetType;
        this.data = data != null ? data.clone() : null;
    }

    public Class<?> getTargetType() {
        return targetType;
    }

    public byte[] getData() {
        return data != null ? data.clone() : null;
    }
}
