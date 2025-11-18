package com.kafka.sdk.resilience;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Message deduplication with support for in-memory and external stores.
 */
public class Deduplicator<K, V> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Deduplicator.class);

    private final DeduplicationStore store;
    private final Function<ConsumerRecord<K, V>, String> idExtractor;
    private final Duration windowDuration;

    public Deduplicator(Duration windowDuration) {
        this(new InMemoryDeduplicationStore(), record -> extractDefaultId(record), windowDuration);
    }

    public Deduplicator(
            DeduplicationStore store,
            Function<ConsumerRecord<K, V>, String> idExtractor,
            Duration windowDuration) {
        this.store = store;
        this.idExtractor = idExtractor;
        this.windowDuration = windowDuration;
    }

    /**
     * Check if a record is a duplicate.
     */
    public boolean isDuplicate(ConsumerRecord<K, V> record) {
        String messageId = idExtractor.apply(record);
        if (messageId == null) {
            return false;
        }
        return store.contains(messageId);
    }

    /**
     * Mark a record as processed.
     */
    public void markProcessed(ConsumerRecord<K, V> record) {
        String messageId = idExtractor.apply(record);
        if (messageId != null) {
            store.put(messageId, windowDuration);
        }
    }

    /**
     * Check and mark in one operation (atomic if supported by store).
     */
    public boolean checkAndMark(ConsumerRecord<K, V> record) {
        String messageId = idExtractor.apply(record);
        if (messageId == null) {
            return false;
        }
        return store.putIfAbsent(messageId, windowDuration);
    }

    /**
     * Get deduplication statistics.
     */
    public DeduplicationStats getStats() {
        return store.getStats();
    }

    /**
     * Clear the deduplication store.
     */
    public void clear() {
        store.clear();
    }

    @Override
    public void close() {
        store.close();
    }

    private static <K, V> String extractDefaultId(ConsumerRecord<K, V> record) {
        // Try to get ID from header
        var header = record.headers().lastHeader("x-message-id");
        if (header != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }

        // Try correlation ID
        header = record.headers().lastHeader("x-correlation-id");
        if (header != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }

        // Generate from topic-partition-offset
        return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
    }

    /**
     * Deduplication store interface.
     */
    public interface DeduplicationStore {
        boolean contains(String key);
        void put(String key, Duration ttl);
        boolean putIfAbsent(String key, Duration ttl);
        void clear();
        void close();
        DeduplicationStats getStats();
    }

    /**
     * In-memory deduplication store with automatic cleanup.
     */
    public static class InMemoryDeduplicationStore implements DeduplicationStore {
        private final ConcurrentHashMap<String, Long> store = new ConcurrentHashMap<>();
        private final ScheduledExecutorService cleaner;
        private long duplicatesDetected = 0;
        private long messagesProcessed = 0;

        public InMemoryDeduplicationStore() {
            this.cleaner = Executors.newSingleThreadScheduledExecutor();
            this.cleaner.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
        }

        @Override
        public boolean contains(String key) {
            Long expiry = store.get(key);
            if (expiry != null && expiry > System.currentTimeMillis()) {
                duplicatesDetected++;
                return true;
            }
            return false;
        }

        @Override
        public void put(String key, Duration ttl) {
            store.put(key, System.currentTimeMillis() + ttl.toMillis());
            messagesProcessed++;
        }

        @Override
        public boolean putIfAbsent(String key, Duration ttl) {
            long expiry = System.currentTimeMillis() + ttl.toMillis();
            Long previous = store.putIfAbsent(key, expiry);

            if (previous != null && previous > System.currentTimeMillis()) {
                duplicatesDetected++;
                return false; // Was duplicate
            }

            if (previous != null) {
                // Expired entry, update it
                store.put(key, expiry);
            }

            messagesProcessed++;
            return true; // Not duplicate
        }

        @Override
        public void clear() {
            store.clear();
        }

        @Override
        public void close() {
            cleaner.shutdown();
        }

        @Override
        public DeduplicationStats getStats() {
            return new DeduplicationStats(messagesProcessed, duplicatesDetected, store.size());
        }

        private void cleanup() {
            long now = System.currentTimeMillis();
            store.entrySet().removeIf(entry -> entry.getValue() < now);
        }
    }

    /**
     * Redis-based deduplication store for distributed deduplication.
     */
    public static class RedisDeduplicationStore implements DeduplicationStore {
        private final String host;
        private final int port;
        private final String keyPrefix;
        private long duplicatesDetected = 0;
        private long messagesProcessed = 0;

        // Note: In a real implementation, you would use a Redis client like Jedis or Lettuce
        // This is a placeholder showing the interface
        public RedisDeduplicationStore(String host, int port, String keyPrefix) {
            this.host = host;
            this.port = port;
            this.keyPrefix = keyPrefix;
        }

        @Override
        public boolean contains(String key) {
            // Redis GET operation
            // return redis.exists(keyPrefix + key);
            throw new UnsupportedOperationException(
                "Redis implementation requires Jedis/Lettuce dependency");
        }

        @Override
        public void put(String key, Duration ttl) {
            // Redis SETEX operation
            // redis.setex(keyPrefix + key, ttl.getSeconds(), "1");
            messagesProcessed++;
            throw new UnsupportedOperationException(
                "Redis implementation requires Jedis/Lettuce dependency");
        }

        @Override
        public boolean putIfAbsent(String key, Duration ttl) {
            // Redis SET NX EX operation
            // return redis.set(keyPrefix + key, "1", SetParams.setParams().nx().ex(ttl.getSeconds())) != null;
            throw new UnsupportedOperationException(
                "Redis implementation requires Jedis/Lettuce dependency");
        }

        @Override
        public void clear() {
            // Redis KEYS + DEL operation
        }

        @Override
        public void close() {
            // Close Redis connection
        }

        @Override
        public DeduplicationStats getStats() {
            return new DeduplicationStats(messagesProcessed, duplicatesDetected, 0);
        }
    }

    /**
     * Bloom filter based deduplication for memory-efficient probabilistic deduplication.
     */
    public static class BloomFilterDeduplicationStore implements DeduplicationStore {
        private final java.util.BitSet bitSet;
        private final int size;
        private final int numHashes;
        private long duplicatesDetected = 0;
        private long messagesProcessed = 0;

        public BloomFilterDeduplicationStore(int expectedElements, double falsePositiveRate) {
            this.size = optimalSize(expectedElements, falsePositiveRate);
            this.numHashes = optimalHashes(expectedElements, size);
            this.bitSet = new java.util.BitSet(size);
        }

        @Override
        public boolean contains(String key) {
            for (int i = 0; i < numHashes; i++) {
                int hash = hash(key, i);
                if (!bitSet.get(hash)) {
                    return false;
                }
            }
            duplicatesDetected++;
            return true; // Might be false positive
        }

        @Override
        public void put(String key, Duration ttl) {
            for (int i = 0; i < numHashes; i++) {
                int hash = hash(key, i);
                bitSet.set(hash);
            }
            messagesProcessed++;
        }

        @Override
        public boolean putIfAbsent(String key, Duration ttl) {
            boolean exists = contains(key);
            if (!exists) {
                put(key, ttl);
                return true;
            }
            return false;
        }

        @Override
        public void clear() {
            bitSet.clear();
        }

        @Override
        public void close() {
            // No-op
        }

        @Override
        public DeduplicationStats getStats() {
            return new DeduplicationStats(messagesProcessed, duplicatesDetected, bitSet.cardinality());
        }

        private int hash(String key, int seed) {
            int hash = key.hashCode() ^ seed;
            return Math.abs(hash % size);
        }

        private static int optimalSize(int n, double p) {
            return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
        }

        private static int optimalHashes(int n, int m) {
            return (int) Math.ceil((double) m / n * Math.log(2));
        }
    }

    public record DeduplicationStats(
        long messagesProcessed,
        long duplicatesDetected,
        long storeSize
    ) {
        public double duplicateRate() {
            return messagesProcessed > 0 ?
                (double) duplicatesDetected / messagesProcessed : 0.0;
        }
    }
}
