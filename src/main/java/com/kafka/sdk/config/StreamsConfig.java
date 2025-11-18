package com.kafka.sdk.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Kafka Streams applications.
 */
public class StreamsConfig {

    private final String applicationId;
    private final int numStreamThreads;
    private final long cacheMaxBytesBuffering;
    private final long commitIntervalMs;
    private final String stateDir;
    private final int replicationFactor;
    private final String processingGuarantee;
    private final long pollMs;

    private StreamsConfig(Builder builder) {
        this.applicationId = builder.applicationId;
        this.numStreamThreads = builder.numStreamThreads;
        this.cacheMaxBytesBuffering = builder.cacheMaxBytesBuffering;
        this.commitIntervalMs = builder.commitIntervalMs;
        this.stateDir = builder.stateDir;
        this.replicationFactor = builder.replicationFactor;
        this.processingGuarantee = builder.processingGuarantee;
        this.pollMs = builder.pollMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getApplicationId() {
        return applicationId;
    }

    public int getNumStreamThreads() {
        return numStreamThreads;
    }

    public long getCacheMaxBytesBuffering() {
        return cacheMaxBytesBuffering;
    }

    public long getCommitIntervalMs() {
        return commitIntervalMs;
    }

    public String getStateDir() {
        return stateDir;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getProcessingGuarantee() {
        return processingGuarantee;
    }

    public long getPollMs() {
        return pollMs;
    }

    public Map<String, Object> toProperties() {
        Map<String, Object> props = new HashMap<>();

        if (applicationId != null) {
            props.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        }

        props.put(org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        props.put(org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, cacheMaxBytesBuffering);
        props.put(org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        props.put(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
        props.put(org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        props.put(org.apache.kafka.streams.StreamsConfig.POLL_MS_CONFIG, pollMs);

        return props;
    }

    public static class Builder {
        private String applicationId;
        private int numStreamThreads = 1;
        private long cacheMaxBytesBuffering = 10485760;
        private long commitIntervalMs = 30000;
        private String stateDir = "/tmp/kafka-streams";
        private int replicationFactor = 1;
        private String processingGuarantee = "at_least_once";
        private long pollMs = 100;

        public Builder applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public Builder numStreamThreads(int numStreamThreads) {
            this.numStreamThreads = numStreamThreads;
            return this;
        }

        public Builder cacheMaxBytesBuffering(long cacheMaxBytesBuffering) {
            this.cacheMaxBytesBuffering = cacheMaxBytesBuffering;
            return this;
        }

        public Builder commitIntervalMs(long commitIntervalMs) {
            this.commitIntervalMs = commitIntervalMs;
            return this;
        }

        public Builder stateDir(String stateDir) {
            this.stateDir = stateDir;
            return this;
        }

        public Builder replicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public Builder processingGuarantee(String processingGuarantee) {
            this.processingGuarantee = processingGuarantee;
            return this;
        }

        public Builder pollMs(long pollMs) {
            this.pollMs = pollMs;
            return this;
        }

        public StreamsConfig build() {
            return new StreamsConfig(this);
        }
    }
}
