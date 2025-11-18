package com.kafka.sdk.config;

import java.time.Duration;

/**
 * Configuration for resilience features like circuit breakers, retries, and rate limiting.
 */
public class ResilienceConfig {

    // Circuit Breaker settings
    private final boolean circuitBreakerEnabled;
    private final int circuitBreakerFailureRateThreshold;
    private final int circuitBreakerSlowCallRateThreshold;
    private final Duration circuitBreakerSlowCallDurationThreshold;
    private final int circuitBreakerPermittedCallsInHalfOpen;
    private final int circuitBreakerMinimumNumberOfCalls;
    private final Duration circuitBreakerWaitDurationInOpenState;

    // Retry settings
    private final boolean retryEnabled;
    private final int retryMaxAttempts;
    private final Duration retryWaitDuration;
    private final double retryExponentialBackoffMultiplier;
    private final Duration retryMaxWaitDuration;

    // Rate Limiter settings
    private final boolean rateLimiterEnabled;
    private final int rateLimiterLimitForPeriod;
    private final Duration rateLimiterLimitRefreshPeriod;
    private final Duration rateLimiterTimeoutDuration;

    // Bulkhead settings
    private final boolean bulkheadEnabled;
    private final int bulkheadMaxConcurrentCalls;
    private final int bulkheadMaxWaitDuration;

    private ResilienceConfig(Builder builder) {
        this.circuitBreakerEnabled = builder.circuitBreakerEnabled;
        this.circuitBreakerFailureRateThreshold = builder.circuitBreakerFailureRateThreshold;
        this.circuitBreakerSlowCallRateThreshold = builder.circuitBreakerSlowCallRateThreshold;
        this.circuitBreakerSlowCallDurationThreshold = builder.circuitBreakerSlowCallDurationThreshold;
        this.circuitBreakerPermittedCallsInHalfOpen = builder.circuitBreakerPermittedCallsInHalfOpen;
        this.circuitBreakerMinimumNumberOfCalls = builder.circuitBreakerMinimumNumberOfCalls;
        this.circuitBreakerWaitDurationInOpenState = builder.circuitBreakerWaitDurationInOpenState;
        this.retryEnabled = builder.retryEnabled;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.retryWaitDuration = builder.retryWaitDuration;
        this.retryExponentialBackoffMultiplier = builder.retryExponentialBackoffMultiplier;
        this.retryMaxWaitDuration = builder.retryMaxWaitDuration;
        this.rateLimiterEnabled = builder.rateLimiterEnabled;
        this.rateLimiterLimitForPeriod = builder.rateLimiterLimitForPeriod;
        this.rateLimiterLimitRefreshPeriod = builder.rateLimiterLimitRefreshPeriod;
        this.rateLimiterTimeoutDuration = builder.rateLimiterTimeoutDuration;
        this.bulkheadEnabled = builder.bulkheadEnabled;
        this.bulkheadMaxConcurrentCalls = builder.bulkheadMaxConcurrentCalls;
        this.bulkheadMaxWaitDuration = builder.bulkheadMaxWaitDuration;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isCircuitBreakerEnabled() {
        return circuitBreakerEnabled;
    }

    public int getCircuitBreakerFailureRateThreshold() {
        return circuitBreakerFailureRateThreshold;
    }

    public int getCircuitBreakerSlowCallRateThreshold() {
        return circuitBreakerSlowCallRateThreshold;
    }

    public Duration getCircuitBreakerSlowCallDurationThreshold() {
        return circuitBreakerSlowCallDurationThreshold;
    }

    public int getCircuitBreakerPermittedCallsInHalfOpen() {
        return circuitBreakerPermittedCallsInHalfOpen;
    }

    public int getCircuitBreakerMinimumNumberOfCalls() {
        return circuitBreakerMinimumNumberOfCalls;
    }

    public Duration getCircuitBreakerWaitDurationInOpenState() {
        return circuitBreakerWaitDurationInOpenState;
    }

    public boolean isRetryEnabled() {
        return retryEnabled;
    }

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public Duration getRetryWaitDuration() {
        return retryWaitDuration;
    }

    public double getRetryExponentialBackoffMultiplier() {
        return retryExponentialBackoffMultiplier;
    }

    public Duration getRetryMaxWaitDuration() {
        return retryMaxWaitDuration;
    }

    public boolean isRateLimiterEnabled() {
        return rateLimiterEnabled;
    }

    public int getRateLimiterLimitForPeriod() {
        return rateLimiterLimitForPeriod;
    }

    public Duration getRateLimiterLimitRefreshPeriod() {
        return rateLimiterLimitRefreshPeriod;
    }

    public Duration getRateLimiterTimeoutDuration() {
        return rateLimiterTimeoutDuration;
    }

    public boolean isBulkheadEnabled() {
        return bulkheadEnabled;
    }

    public int getBulkheadMaxConcurrentCalls() {
        return bulkheadMaxConcurrentCalls;
    }

    public int getBulkheadMaxWaitDuration() {
        return bulkheadMaxWaitDuration;
    }

    public static class Builder {
        // Circuit Breaker defaults
        private boolean circuitBreakerEnabled = true;
        private int circuitBreakerFailureRateThreshold = 50;
        private int circuitBreakerSlowCallRateThreshold = 100;
        private Duration circuitBreakerSlowCallDurationThreshold = Duration.ofSeconds(60);
        private int circuitBreakerPermittedCallsInHalfOpen = 10;
        private int circuitBreakerMinimumNumberOfCalls = 10;
        private Duration circuitBreakerWaitDurationInOpenState = Duration.ofSeconds(60);

        // Retry defaults
        private boolean retryEnabled = true;
        private int retryMaxAttempts = 3;
        private Duration retryWaitDuration = Duration.ofMillis(500);
        private double retryExponentialBackoffMultiplier = 2.0;
        private Duration retryMaxWaitDuration = Duration.ofSeconds(10);

        // Rate Limiter defaults
        private boolean rateLimiterEnabled = false;
        private int rateLimiterLimitForPeriod = 100000;
        private Duration rateLimiterLimitRefreshPeriod = Duration.ofSeconds(1);
        private Duration rateLimiterTimeoutDuration = Duration.ofSeconds(5);

        // Bulkhead defaults
        private boolean bulkheadEnabled = false;
        private int bulkheadMaxConcurrentCalls = 25;
        private int bulkheadMaxWaitDuration = 0;

        public Builder circuitBreakerEnabled(boolean circuitBreakerEnabled) {
            this.circuitBreakerEnabled = circuitBreakerEnabled;
            return this;
        }

        public Builder circuitBreakerFailureRateThreshold(int threshold) {
            this.circuitBreakerFailureRateThreshold = threshold;
            return this;
        }

        public Builder circuitBreakerSlowCallRateThreshold(int threshold) {
            this.circuitBreakerSlowCallRateThreshold = threshold;
            return this;
        }

        public Builder circuitBreakerSlowCallDurationThreshold(Duration duration) {
            this.circuitBreakerSlowCallDurationThreshold = duration;
            return this;
        }

        public Builder circuitBreakerPermittedCallsInHalfOpen(int calls) {
            this.circuitBreakerPermittedCallsInHalfOpen = calls;
            return this;
        }

        public Builder circuitBreakerMinimumNumberOfCalls(int calls) {
            this.circuitBreakerMinimumNumberOfCalls = calls;
            return this;
        }

        public Builder circuitBreakerWaitDurationInOpenState(Duration duration) {
            this.circuitBreakerWaitDurationInOpenState = duration;
            return this;
        }

        public Builder retryEnabled(boolean retryEnabled) {
            this.retryEnabled = retryEnabled;
            return this;
        }

        public Builder retryMaxAttempts(int maxAttempts) {
            this.retryMaxAttempts = maxAttempts;
            return this;
        }

        public Builder retryWaitDuration(Duration duration) {
            this.retryWaitDuration = duration;
            return this;
        }

        public Builder retryExponentialBackoffMultiplier(double multiplier) {
            this.retryExponentialBackoffMultiplier = multiplier;
            return this;
        }

        public Builder retryMaxWaitDuration(Duration duration) {
            this.retryMaxWaitDuration = duration;
            return this;
        }

        public Builder rateLimiterEnabled(boolean enabled) {
            this.rateLimiterEnabled = enabled;
            return this;
        }

        public Builder rateLimiterLimitForPeriod(int limit) {
            this.rateLimiterLimitForPeriod = limit;
            return this;
        }

        public Builder rateLimiterLimitRefreshPeriod(Duration period) {
            this.rateLimiterLimitRefreshPeriod = period;
            return this;
        }

        public Builder rateLimiterTimeoutDuration(Duration duration) {
            this.rateLimiterTimeoutDuration = duration;
            return this;
        }

        public Builder bulkheadEnabled(boolean enabled) {
            this.bulkheadEnabled = enabled;
            return this;
        }

        public Builder bulkheadMaxConcurrentCalls(int calls) {
            this.bulkheadMaxConcurrentCalls = calls;
            return this;
        }

        public Builder bulkheadMaxWaitDuration(int duration) {
            this.bulkheadMaxWaitDuration = duration;
            return this;
        }

        public ResilienceConfig build() {
            return new ResilienceConfig(this);
        }
    }
}
