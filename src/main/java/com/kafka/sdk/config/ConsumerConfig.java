package com.kafka.sdk.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Kafka consumers.
 */
public class ConsumerConfig {

    private final String groupId;
    private final AutoOffsetReset autoOffsetReset;
    private final boolean enableAutoCommit;
    private final int autoCommitIntervalMs;
    private final int sessionTimeoutMs;
    private final int heartbeatIntervalMs;
    private final int maxPollRecords;
    private final int maxPollIntervalMs;
    private final int fetchMinBytes;
    private final int fetchMaxBytes;
    private final int fetchMaxWaitMs;
    private final String keyDeserializer;
    private final String valueDeserializer;
    private final IsolationLevel isolationLevel;
    private final String partitionAssignmentStrategy;
    private final String clientId;
    private final String groupInstanceId;

    private ConsumerConfig(Builder builder) {
        this.groupId = builder.groupId;
        this.autoOffsetReset = builder.autoOffsetReset;
        this.enableAutoCommit = builder.enableAutoCommit;
        this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
        this.sessionTimeoutMs = builder.sessionTimeoutMs;
        this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
        this.maxPollRecords = builder.maxPollRecords;
        this.maxPollIntervalMs = builder.maxPollIntervalMs;
        this.fetchMinBytes = builder.fetchMinBytes;
        this.fetchMaxBytes = builder.fetchMaxBytes;
        this.fetchMaxWaitMs = builder.fetchMaxWaitMs;
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
        this.isolationLevel = builder.isolationLevel;
        this.partitionAssignmentStrategy = builder.partitionAssignmentStrategy;
        this.clientId = builder.clientId;
        this.groupInstanceId = builder.groupInstanceId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getGroupId() {
        return groupId;
    }

    public AutoOffsetReset getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public int getFetchMinBytes() {
        return fetchMinBytes;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public int getFetchMaxWaitMs() {
        return fetchMaxWaitMs;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public String getPartitionAssignmentStrategy() {
        return partitionAssignmentStrategy;
    }

    public String getClientId() {
        return clientId;
    }

    public String getGroupInstanceId() {
        return groupInstanceId;
    }

    public Map<String, Object> toProperties() {
        Map<String, Object> props = new HashMap<>();

        if (groupId != null) {
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.getValue());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.getValue());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);

        if (clientId != null) {
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }

        if (groupInstanceId != null) {
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        }

        return props;
    }

    public enum AutoOffsetReset {
        EARLIEST("earliest"),
        LATEST("latest"),
        NONE("none");

        private final String value;

        AutoOffsetReset(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public enum IsolationLevel {
        READ_UNCOMMITTED("read_uncommitted"),
        READ_COMMITTED("read_committed");

        private final String value;

        IsolationLevel(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static class Builder {
        private String groupId;
        private AutoOffsetReset autoOffsetReset = AutoOffsetReset.LATEST;
        private boolean enableAutoCommit = false;
        private int autoCommitIntervalMs = 5000;
        private int sessionTimeoutMs = 45000;
        private int heartbeatIntervalMs = 3000;
        private int maxPollRecords = 500;
        private int maxPollIntervalMs = 300000;
        private int fetchMinBytes = 1;
        private int fetchMaxBytes = 52428800;
        private int fetchMaxWaitMs = 500;
        private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
        private String partitionAssignmentStrategy = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
        private String clientId;
        private String groupInstanceId;

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder autoOffsetReset(AutoOffsetReset autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
            return this;
        }

        public Builder enableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder autoCommitIntervalMs(int autoCommitIntervalMs) {
            this.autoCommitIntervalMs = autoCommitIntervalMs;
            return this;
        }

        public Builder sessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        public Builder heartbeatIntervalMs(int heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public Builder maxPollIntervalMs(int maxPollIntervalMs) {
            this.maxPollIntervalMs = maxPollIntervalMs;
            return this;
        }

        public Builder fetchMinBytes(int fetchMinBytes) {
            this.fetchMinBytes = fetchMinBytes;
            return this;
        }

        public Builder fetchMaxBytes(int fetchMaxBytes) {
            this.fetchMaxBytes = fetchMaxBytes;
            return this;
        }

        public Builder fetchMaxWaitMs(int fetchMaxWaitMs) {
            this.fetchMaxWaitMs = fetchMaxWaitMs;
            return this;
        }

        public Builder keyDeserializer(String keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        public Builder valueDeserializer(String valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        public Builder isolationLevel(IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        public Builder partitionAssignmentStrategy(String partitionAssignmentStrategy) {
            this.partitionAssignmentStrategy = partitionAssignmentStrategy;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder groupInstanceId(String groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
