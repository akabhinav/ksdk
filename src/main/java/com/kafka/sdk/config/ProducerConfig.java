package com.kafka.sdk.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Kafka producers.
 */
public class ProducerConfig {

    private final Acks acks;
    private final int retries;
    private final int batchSize;
    private final long lingerMs;
    private final long bufferMemory;
    private final CompressionType compressionType;
    private final String keySerializer;
    private final String valueSerializer;
    private final boolean enableIdempotence;
    private final String transactionalId;
    private final int maxInFlightRequestsPerConnection;
    private final int deliveryTimeoutMs;
    private final int requestTimeoutMs;
    private final int maxBlockMs;

    private ProducerConfig(Builder builder) {
        this.acks = builder.acks;
        this.retries = builder.retries;
        this.batchSize = builder.batchSize;
        this.lingerMs = builder.lingerMs;
        this.bufferMemory = builder.bufferMemory;
        this.compressionType = builder.compressionType;
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.enableIdempotence = builder.enableIdempotence;
        this.transactionalId = builder.transactionalId;
        this.maxInFlightRequestsPerConnection = builder.maxInFlightRequestsPerConnection;
        this.deliveryTimeoutMs = builder.deliveryTimeoutMs;
        this.requestTimeoutMs = builder.requestTimeoutMs;
        this.maxBlockMs = builder.maxBlockMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Acks getAcks() {
        return acks;
    }

    public int getRetries() {
        return retries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getLingerMs() {
        return lingerMs;
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public boolean isEnableIdempotence() {
        return enableIdempotence;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public int getMaxInFlightRequestsPerConnection() {
        return maxInFlightRequestsPerConnection;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getMaxBlockMs() {
        return maxBlockMs;
    }

    public Map<String, Object> toProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, acks.getValue());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, retries);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType.getValue());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);

        if (transactionalId != null && !transactionalId.isEmpty()) {
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }

        return props;
    }

    public enum Acks {
        NONE("0"),
        LEADER("1"),
        ALL("all");

        private final String value;

        Acks(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public enum CompressionType {
        NONE("none"),
        GZIP("gzip"),
        SNAPPY("snappy"),
        LZ4("lz4"),
        ZSTD("zstd");

        private final String value;

        CompressionType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static class Builder {
        private Acks acks = Acks.ALL;
        private int retries = 3;
        private int batchSize = 16384;
        private long lingerMs = 1;
        private long bufferMemory = 33554432;
        private CompressionType compressionType = CompressionType.SNAPPY;
        private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
        private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
        private boolean enableIdempotence = true;
        private String transactionalId;
        private int maxInFlightRequestsPerConnection = 5;
        private int deliveryTimeoutMs = 120000;
        private int requestTimeoutMs = 30000;
        private int maxBlockMs = 60000;

        public Builder acks(Acks acks) {
            this.acks = acks;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder lingerMs(long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder bufferMemory(long bufferMemory) {
            this.bufferMemory = bufferMemory;
            return this;
        }

        public Builder compressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder keySerializer(String keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder valueSerializer(String valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public Builder enableIdempotence(boolean enableIdempotence) {
            this.enableIdempotence = enableIdempotence;
            return this;
        }

        public Builder transactionalId(String transactionalId) {
            this.transactionalId = transactionalId;
            return this;
        }

        public Builder maxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
            this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
            return this;
        }

        public Builder deliveryTimeoutMs(int deliveryTimeoutMs) {
            this.deliveryTimeoutMs = deliveryTimeoutMs;
            return this;
        }

        public Builder requestTimeoutMs(int requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        public Builder maxBlockMs(int maxBlockMs) {
            this.maxBlockMs = maxBlockMs;
            return this;
        }

        public ProducerConfig build() {
            return new ProducerConfig(this);
        }
    }
}
