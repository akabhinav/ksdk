package com.kafka.sdk.config;

import java.time.Duration;

/**
 * Configuration for monitoring and observability features.
 */
public class MonitoringConfig {

    private final boolean metricsEnabled;
    private final boolean tracingEnabled;
    private final String metricsPrefix;
    private final Duration metricsSampleWindow;
    private final int metricsNumSamples;
    private final boolean prometheusEnabled;
    private final int prometheusPort;
    private final String prometheusEndpoint;
    private final boolean healthCheckEnabled;
    private final Duration healthCheckInterval;

    private MonitoringConfig(Builder builder) {
        this.metricsEnabled = builder.metricsEnabled;
        this.tracingEnabled = builder.tracingEnabled;
        this.metricsPrefix = builder.metricsPrefix;
        this.metricsSampleWindow = builder.metricsSampleWindow;
        this.metricsNumSamples = builder.metricsNumSamples;
        this.prometheusEnabled = builder.prometheusEnabled;
        this.prometheusPort = builder.prometheusPort;
        this.prometheusEndpoint = builder.prometheusEndpoint;
        this.healthCheckEnabled = builder.healthCheckEnabled;
        this.healthCheckInterval = builder.healthCheckInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    public Duration getMetricsSampleWindow() {
        return metricsSampleWindow;
    }

    public int getMetricsNumSamples() {
        return metricsNumSamples;
    }

    public boolean isPrometheusEnabled() {
        return prometheusEnabled;
    }

    public int getPrometheusPort() {
        return prometheusPort;
    }

    public String getPrometheusEndpoint() {
        return prometheusEndpoint;
    }

    public boolean isHealthCheckEnabled() {
        return healthCheckEnabled;
    }

    public Duration getHealthCheckInterval() {
        return healthCheckInterval;
    }

    public static class Builder {
        private boolean metricsEnabled = true;
        private boolean tracingEnabled = true;
        private String metricsPrefix = "kafka.sdk";
        private Duration metricsSampleWindow = Duration.ofSeconds(30);
        private int metricsNumSamples = 2;
        private boolean prometheusEnabled = true;
        private int prometheusPort = 9090;
        private String prometheusEndpoint = "/metrics";
        private boolean healthCheckEnabled = true;
        private Duration healthCheckInterval = Duration.ofSeconds(30);

        public Builder metricsEnabled(boolean metricsEnabled) {
            this.metricsEnabled = metricsEnabled;
            return this;
        }

        public Builder tracingEnabled(boolean tracingEnabled) {
            this.tracingEnabled = tracingEnabled;
            return this;
        }

        public Builder metricsPrefix(String metricsPrefix) {
            this.metricsPrefix = metricsPrefix;
            return this;
        }

        public Builder metricsSampleWindow(Duration metricsSampleWindow) {
            this.metricsSampleWindow = metricsSampleWindow;
            return this;
        }

        public Builder metricsNumSamples(int metricsNumSamples) {
            this.metricsNumSamples = metricsNumSamples;
            return this;
        }

        public Builder prometheusEnabled(boolean prometheusEnabled) {
            this.prometheusEnabled = prometheusEnabled;
            return this;
        }

        public Builder prometheusPort(int prometheusPort) {
            this.prometheusPort = prometheusPort;
            return this;
        }

        public Builder prometheusEndpoint(String prometheusEndpoint) {
            this.prometheusEndpoint = prometheusEndpoint;
            return this;
        }

        public Builder healthCheckEnabled(boolean healthCheckEnabled) {
            this.healthCheckEnabled = healthCheckEnabled;
            return this;
        }

        public Builder healthCheckInterval(Duration healthCheckInterval) {
            this.healthCheckInterval = healthCheckInterval;
            return this;
        }

        public MonitoringConfig build() {
            return new MonitoringConfig(this);
        }
    }
}
