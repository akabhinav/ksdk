package com.kafka.sdk.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Kafka admin operations.
 */
public class AdminConfig {

    private final int requestTimeoutMs;
    private final int defaultApiTimeoutMs;
    private final int retries;
    private final long retryBackoffMs;
    private final String clientId;

    private AdminConfig(Builder builder) {
        this.requestTimeoutMs = builder.requestTimeoutMs;
        this.defaultApiTimeoutMs = builder.defaultApiTimeoutMs;
        this.retries = builder.retries;
        this.retryBackoffMs = builder.retryBackoffMs;
        this.clientId = builder.clientId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getDefaultApiTimeoutMs() {
        return defaultApiTimeoutMs;
    }

    public int getRetries() {
        return retries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public String getClientId() {
        return clientId;
    }

    public Map<String, Object> toProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.RETRIES_CONFIG, retries);
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);

        if (clientId != null) {
            props.put(org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        }

        return props;
    }

    public static class Builder {
        private int requestTimeoutMs = 30000;
        private int defaultApiTimeoutMs = 60000;
        private int retries = 5;
        private long retryBackoffMs = 100;
        private String clientId;

        public Builder requestTimeoutMs(int requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        public Builder defaultApiTimeoutMs(int defaultApiTimeoutMs) {
            this.defaultApiTimeoutMs = defaultApiTimeoutMs;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder retryBackoffMs(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public AdminConfig build() {
            return new AdminConfig(this);
        }
    }
}
