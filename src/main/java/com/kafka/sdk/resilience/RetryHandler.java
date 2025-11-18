package com.kafka.sdk.resilience;

import com.kafka.sdk.config.ResilienceConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Retry handler with exponential backoff support.
 */
public class RetryHandler {

    private static final Logger logger = LoggerFactory.getLogger(RetryHandler.class);

    private final Retry retry;
    private final int maxAttempts;
    private final Duration waitDuration;

    public RetryHandler(ResilienceConfig config) {
        this.maxAttempts = config.getRetryMaxAttempts();
        this.waitDuration = config.getRetryWaitDuration();

        RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(maxAttempts)
            .waitDuration(waitDuration)
            .retryOnException(e -> true)
            .build();

        this.retry = Retry.of("kafka-sdk-retry", retryConfig);
    }

    public RetryHandler(int maxAttempts, Duration waitDuration) {
        this.maxAttempts = maxAttempts;
        this.waitDuration = waitDuration;

        RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(maxAttempts)
            .waitDuration(waitDuration)
            .retryOnException(e -> true)
            .build();

        this.retry = Retry.of("kafka-sdk-retry", retryConfig);
    }

    /**
     * Execute with retry logic.
     */
    public <T> T execute(Supplier<T> supplier) {
        return Retry.decorateSupplier(retry, supplier).get();
    }

    /**
     * Execute with retry logic (void return).
     */
    public void execute(Runnable runnable) {
        Retry.decorateRunnable(retry, runnable).run();
    }

    /**
     * Execute with custom retry configuration.
     */
    public <T> T executeWithConfig(Supplier<T> supplier, int maxAttempts, Duration waitDuration) {
        RetryConfig customConfig = RetryConfig.custom()
            .maxAttempts(maxAttempts)
            .waitDuration(waitDuration)
            .retryOnException(e -> true)
            .build();

        Retry customRetry = Retry.of("custom-retry", customConfig);
        return Retry.decorateSupplier(customRetry, supplier).get();
    }

    /**
     * Execute with exponential backoff.
     */
    public <T> T executeWithExponentialBackoff(
            Supplier<T> supplier,
            int maxAttempts,
            Duration initialWait,
            double multiplier) {

        RetryConfig config = RetryConfig.custom()
            .maxAttempts(maxAttempts)
            .waitDuration(initialWait)
            .retryOnException(e -> true)
            .build();

        Retry retry = Retry.of("exponential-retry", config);

        int attempt = 0;
        Exception lastException = null;
        Duration currentWait = initialWait;

        while (attempt < maxAttempts) {
            try {
                return supplier.get();
            } catch (Exception e) {
                lastException = e;
                attempt++;
                if (attempt < maxAttempts) {
                    logger.warn("Attempt {} failed, retrying in {}ms", attempt, currentWait.toMillis());
                    try {
                        Thread.sleep(currentWait.toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                    currentWait = Duration.ofMillis((long) (currentWait.toMillis() * multiplier));
                }
            }
        }

        throw new RuntimeException("All retry attempts failed", lastException);
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Duration getWaitDuration() {
        return waitDuration;
    }
}
