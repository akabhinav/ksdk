package com.kafka.sdk.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Adaptive backpressure management with automatic throttling based on system metrics.
 */
public class AdaptiveBackpressure implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AdaptiveBackpressure.class);

    private final Config config;
    private final Semaphore permits;
    private final AtomicInteger currentPermits;
    private final AtomicLong successCount;
    private final AtomicLong failureCount;
    private final AtomicLong rejectedCount;
    private final ScheduledExecutorService scheduler;
    private final BlockingQueue<Long> latencyWindow;

    public AdaptiveBackpressure(Config config) {
        this.config = config;
        this.permits = new Semaphore(config.initialPermits());
        this.currentPermits = new AtomicInteger(config.initialPermits());
        this.successCount = new AtomicLong(0);
        this.failureCount = new AtomicLong(0);
        this.rejectedCount = new AtomicLong(0);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.latencyWindow = new LinkedBlockingQueue<>(config.latencyWindowSize());

        // Start adaptive adjustment
        scheduler.scheduleAtFixedRate(
            this::adjustPermits,
            config.adjustmentInterval().toMillis(),
            config.adjustmentInterval().toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Execute an operation with backpressure control.
     */
    public <T> T execute(Supplier<T> operation) throws BackpressureException {
        if (!tryAcquire()) {
            rejectedCount.incrementAndGet();
            throw new BackpressureException("System is under pressure, request rejected");
        }

        long startTime = System.nanoTime();
        try {
            T result = operation.get();
            successCount.incrementAndGet();
            return result;
        } catch (Exception e) {
            failureCount.incrementAndGet();
            throw e;
        } finally {
            long latency = System.nanoTime() - startTime;
            recordLatency(latency);
            permits.release();
        }
    }

    /**
     * Execute an operation with backpressure control (async).
     */
    public <T> CompletableFuture<T> executeAsync(Supplier<CompletableFuture<T>> operation) {
        if (!tryAcquire()) {
            rejectedCount.incrementAndGet();
            return CompletableFuture.failedFuture(
                new BackpressureException("System is under pressure, request rejected"));
        }

        long startTime = System.nanoTime();
        return operation.get()
            .whenComplete((result, error) -> {
                long latency = System.nanoTime() - startTime;
                recordLatency(latency);
                permits.release();

                if (error != null) {
                    failureCount.incrementAndGet();
                } else {
                    successCount.incrementAndGet();
                }
            });
    }

    /**
     * Try to acquire a permit without blocking.
     */
    public boolean tryAcquire() {
        return permits.tryAcquire();
    }

    /**
     * Try to acquire a permit with timeout.
     */
    public boolean tryAcquire(Duration timeout) throws InterruptedException {
        return permits.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Get current statistics.
     */
    public BackpressureStats getStats() {
        double avgLatency = calculateAverageLatency();
        double p99Latency = calculatePercentileLatency(0.99);
        int available = permits.availablePermits();

        return new BackpressureStats(
            currentPermits.get(),
            available,
            successCount.get(),
            failureCount.get(),
            rejectedCount.get(),
            avgLatency,
            p99Latency
        );
    }

    /**
     * Reset statistics.
     */
    public void resetStats() {
        successCount.set(0);
        failureCount.set(0);
        rejectedCount.set(0);
        latencyWindow.clear();
    }

    private void recordLatency(long latencyNanos) {
        // Remove oldest if at capacity
        if (latencyWindow.remainingCapacity() == 0) {
            latencyWindow.poll();
        }
        latencyWindow.offer(latencyNanos);
    }

    private void adjustPermits() {
        try {
            BackpressureStats stats = getStats();

            // Calculate error rate
            long totalRequests = stats.successCount() + stats.failureCount();
            double errorRate = totalRequests > 0 ?
                (double) stats.failureCount() / totalRequests : 0.0;

            // Get current latency metrics
            double avgLatency = stats.averageLatencyMs();
            double p99Latency = stats.p99LatencyMs();

            int adjustment = 0;

            // Decrease permits if system is struggling
            if (errorRate > config.errorRateThreshold()) {
                adjustment = -config.permitAdjustmentStep();
                logger.debug("Decreasing permits due to high error rate: {}", errorRate);
            } else if (p99Latency > config.latencyThresholdMs()) {
                adjustment = -config.permitAdjustmentStep();
                logger.debug("Decreasing permits due to high latency: {}ms", p99Latency);
            } else if (avgLatency < config.latencyThresholdMs() * 0.5 &&
                       errorRate < config.errorRateThreshold() * 0.5) {
                // Increase permits if system is healthy
                adjustment = config.permitAdjustmentStep();
                logger.debug("Increasing permits due to good performance");
            }

            if (adjustment != 0) {
                int newPermits = Math.max(config.minPermits(),
                    Math.min(config.maxPermits(), currentPermits.get() + adjustment));

                int delta = newPermits - currentPermits.get();
                if (delta > 0) {
                    permits.release(delta);
                } else if (delta < 0) {
                    // Drain excess permits
                    permits.drainPermits();
                    permits.release(newPermits);
                }

                currentPermits.set(newPermits);
                logger.info("Adjusted permits from {} to {} (delta: {})",
                    currentPermits.get() - delta, newPermits, delta);
            }

            // Reset counters for next window
            successCount.set(0);
            failureCount.set(0);

        } catch (Exception e) {
            logger.error("Error adjusting permits", e);
        }
    }

    private double calculateAverageLatency() {
        if (latencyWindow.isEmpty()) {
            return 0.0;
        }
        return latencyWindow.stream()
            .mapToLong(Long::longValue)
            .average()
            .orElse(0.0) / 1_000_000.0; // Convert to ms
    }

    private double calculatePercentileLatency(double percentile) {
        if (latencyWindow.isEmpty()) {
            return 0.0;
        }
        long[] sorted = latencyWindow.stream()
            .mapToLong(Long::longValue)
            .sorted()
            .toArray();
        int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, index)] / 1_000_000.0; // Convert to ms
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public record Config(
        int initialPermits,
        int minPermits,
        int maxPermits,
        int permitAdjustmentStep,
        double errorRateThreshold,
        double latencyThresholdMs,
        Duration adjustmentInterval,
        int latencyWindowSize
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int initialPermits = 100;
            private int minPermits = 10;
            private int maxPermits = 1000;
            private int permitAdjustmentStep = 10;
            private double errorRateThreshold = 0.1;
            private double latencyThresholdMs = 1000.0;
            private Duration adjustmentInterval = Duration.ofSeconds(10);
            private int latencyWindowSize = 1000;

            public Builder initialPermits(int permits) {
                this.initialPermits = permits;
                return this;
            }

            public Builder minPermits(int permits) {
                this.minPermits = permits;
                return this;
            }

            public Builder maxPermits(int permits) {
                this.maxPermits = permits;
                return this;
            }

            public Builder permitAdjustmentStep(int step) {
                this.permitAdjustmentStep = step;
                return this;
            }

            public Builder errorRateThreshold(double threshold) {
                this.errorRateThreshold = threshold;
                return this;
            }

            public Builder latencyThresholdMs(double threshold) {
                this.latencyThresholdMs = threshold;
                return this;
            }

            public Builder adjustmentInterval(Duration interval) {
                this.adjustmentInterval = interval;
                return this;
            }

            public Builder latencyWindowSize(int size) {
                this.latencyWindowSize = size;
                return this;
            }

            public Config build() {
                return new Config(
                    initialPermits, minPermits, maxPermits, permitAdjustmentStep,
                    errorRateThreshold, latencyThresholdMs, adjustmentInterval, latencyWindowSize
                );
            }
        }
    }

    public record BackpressureStats(
        int currentPermits,
        int availablePermits,
        long successCount,
        long failureCount,
        long rejectedCount,
        double averageLatencyMs,
        double p99LatencyMs
    ) {
        public double utilizationRate() {
            return currentPermits > 0 ?
                (double) (currentPermits - availablePermits) / currentPermits : 0.0;
        }
    }

    public static class BackpressureException extends RuntimeException {
        public BackpressureException(String message) {
            super(message);
        }
    }
}
