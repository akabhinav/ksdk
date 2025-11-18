package com.kafka.sdk.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom partitioning strategies for message distribution.
 */
public class PartitionStrategy {

    /**
     * Round-robin partitioner that distributes messages evenly across partitions.
     */
    public static class RoundRobinPartitioner implements Partitioner {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                           Object value, byte[] valueBytes, Cluster cluster) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            return Math.abs(counter.getAndIncrement() % numPartitions);
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * Sticky partitioner that sends messages to the same partition until batch is full.
     */
    public static class StickyPartitioner implements Partitioner {
        private final AtomicInteger currentPartition = new AtomicInteger(-1);
        private final AtomicInteger messageCount = new AtomicInteger(0);
        private final int batchSize;

        public StickyPartitioner() {
            this.batchSize = 100;
        }

        public StickyPartitioner(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                           Object value, byte[] valueBytes, Cluster cluster) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();

            if (currentPartition.get() == -1 || messageCount.incrementAndGet() > batchSize) {
                currentPartition.set((currentPartition.get() + 1) % numPartitions);
                messageCount.set(1);
            }

            return currentPartition.get();
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * Hash-based partitioner using murmur2 hash.
     */
    public static class HashPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                           Object value, byte[] valueBytes, Cluster cluster) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();

            if (keyBytes == null) {
                return 0;
            }

            return Math.abs(murmur2(keyBytes) % numPartitions);
        }

        private int murmur2(byte[] data) {
            int length = data.length;
            int seed = 0x9747b28c;
            int m = 0x5bd1e995;
            int r = 24;

            int h = seed ^ length;
            int length4 = length / 4;

            for (int i = 0; i < length4; i++) {
                int i4 = i * 4;
                int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) +
                        ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
                k *= m;
                k ^= k >>> r;
                k *= m;
                h *= m;
                h ^= k;
            }

            switch (length % 4) {
                case 3:
                    h ^= (data[(length & ~3) + 2] & 0xff) << 16;
                case 2:
                    h ^= (data[(length & ~3) + 1] & 0xff) << 8;
                case 1:
                    h ^= data[length & ~3] & 0xff;
                    h *= m;
            }

            h ^= h >>> 13;
            h *= m;
            h ^= h >>> 15;

            return h;
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * Business logic partitioner that extracts partition from the message value.
     */
    public abstract static class BusinessLogicPartitioner<V> implements Partitioner {

        protected abstract int extractPartition(V value, int numPartitions);

        @Override
        @SuppressWarnings("unchecked")
        public int partition(String topic, Object key, byte[] keyBytes,
                           Object value, byte[] valueBytes, Cluster cluster) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();

            if (value == null) {
                return 0;
            }

            return extractPartition((V) value, numPartitions);
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }
}
