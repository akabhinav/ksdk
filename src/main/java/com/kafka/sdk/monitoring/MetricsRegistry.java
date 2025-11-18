package com.kafka.sdk.monitoring;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Central registry for all SDK metrics.
 */
public class MetricsRegistry {

    private static final MeterRegistry registry = new SimpleMeterRegistry();
    private static final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Gauge> gauges = new ConcurrentHashMap<>();

    /**
     * Get the meter registry.
     */
    public static MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Create or get a counter.
     */
    public static Counter counter(String name, String... tags) {
        return counters.computeIfAbsent(name, k ->
            Counter.builder(name)
                .tags(tags)
                .register(registry));
    }

    /**
     * Create or get a timer.
     */
    public static Timer timer(String name, String... tags) {
        return timers.computeIfAbsent(name, k ->
            Timer.builder(name)
                .tags(tags)
                .register(registry));
    }

    /**
     * Create or get a gauge.
     */
    public static <T extends Number> Gauge gauge(String name, Supplier<T> valueSupplier, String... tags) {
        return gauges.computeIfAbsent(name, k ->
            Gauge.builder(name, valueSupplier)
                .tags(tags)
                .register(registry));
    }

    /**
     * Record a timer value.
     */
    public static void recordTime(String name, long duration, TimeUnit unit, String... tags) {
        timer(name, tags).record(duration, unit);
    }

    /**
     * Increment a counter.
     */
    public static void increment(String name, String... tags) {
        counter(name, tags).increment();
    }

    /**
     * Increment a counter by a specific amount.
     */
    public static void increment(String name, double amount, String... tags) {
        counter(name, tags).increment(amount);
    }

    /**
     * Create a distribution summary.
     */
    public static DistributionSummary summary(String name, String... tags) {
        return DistributionSummary.builder(name)
            .tags(tags)
            .register(registry);
    }

    /**
     * Clear all metrics.
     */
    public static void clear() {
        counters.clear();
        timers.clear();
        gauges.clear();
        registry.clear();
    }
}
