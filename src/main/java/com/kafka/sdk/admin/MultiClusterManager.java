package com.kafka.sdk.admin;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.consumer.ConsumerFactory;
import com.kafka.sdk.consumer.SmartConsumer;
import com.kafka.sdk.producer.ProducerFactory;
import com.kafka.sdk.producer.SmartProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Multi-cluster management for disaster recovery and cross-datacenter replication.
 */
public class MultiClusterManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MultiClusterManager.class);

    private final Map<String, ClusterConnection> clusters;
    private final Map<String, ReplicationStream> replicationStreams;
    private final ExecutorService executor;

    public MultiClusterManager() {
        this.clusters = new ConcurrentHashMap<>();
        this.replicationStreams = new ConcurrentHashMap<>();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Register a cluster.
     */
    public void registerCluster(String clusterName, KafkaSdkConfig config) {
        clusters.put(clusterName, new ClusterConnection(
            clusterName,
            config,
            new KafkaAdminClient(config),
            ClusterRole.SECONDARY,
            ClusterStatus.CONNECTED
        ));
        logger.info("Registered cluster: {}", clusterName);
    }

    /**
     * Set a cluster as primary.
     */
    public void setPrimary(String clusterName) {
        ClusterConnection cluster = clusters.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterName);
        }

        // Demote current primary
        clusters.values().stream()
            .filter(c -> c.role() == ClusterRole.PRIMARY)
            .forEach(c -> clusters.put(c.name(), c.withRole(ClusterRole.SECONDARY)));

        // Promote new primary
        clusters.put(clusterName, cluster.withRole(ClusterRole.PRIMARY));
        logger.info("Set {} as primary cluster", clusterName);
    }

    /**
     * Get the current primary cluster.
     */
    public Optional<ClusterConnection> getPrimary() {
        return clusters.values().stream()
            .filter(c -> c.role() == ClusterRole.PRIMARY)
            .findFirst();
    }

    /**
     * Start replication between clusters.
     */
    public void startReplication(String sourceCluster, String targetCluster,
                                 List<String> topics, ReplicationMode mode) {
        ClusterConnection source = clusters.get(sourceCluster);
        ClusterConnection target = clusters.get(targetCluster);

        if (source == null || target == null) {
            throw new IllegalArgumentException("Unknown cluster");
        }

        String streamId = sourceCluster + "->" + targetCluster;

        ReplicationStream stream = new ReplicationStream(
            streamId,
            source,
            target,
            topics,
            mode
        );

        replicationStreams.put(streamId, stream);
        executor.submit(stream::start);

        logger.info("Started replication from {} to {} for topics {}",
            sourceCluster, targetCluster, topics);
    }

    /**
     * Stop replication between clusters.
     */
    public void stopReplication(String sourceCluster, String targetCluster) {
        String streamId = sourceCluster + "->" + targetCluster;
        ReplicationStream stream = replicationStreams.remove(streamId);
        if (stream != null) {
            stream.stop();
            logger.info("Stopped replication {}", streamId);
        }
    }

    /**
     * Perform failover to a secondary cluster.
     */
    public void failover(String targetCluster) {
        Optional<ClusterConnection> currentPrimary = getPrimary();

        // Stop all replication to the target
        replicationStreams.values().stream()
            .filter(s -> s.target().name().equals(targetCluster))
            .forEach(ReplicationStream::stop);

        // Set new primary
        setPrimary(targetCluster);

        // Start reverse replication if previous primary exists
        currentPrimary.ifPresent(oldPrimary -> {
            if (oldPrimary.status() == ClusterStatus.CONNECTED) {
                // Could start reverse replication here
                logger.info("Previous primary {} is still available", oldPrimary.name());
            }
        });

        logger.info("Failover completed to {}", targetCluster);
    }

    /**
     * Get cluster health status.
     */
    public Map<String, ClusterHealthReport> getClusterHealth() {
        Map<String, ClusterHealthReport> reports = new HashMap<>();

        for (ClusterConnection cluster : clusters.values()) {
            boolean healthy = cluster.adminClient().isClusterHealthy();
            reports.put(cluster.name(), new ClusterHealthReport(
                cluster.name(),
                cluster.role(),
                healthy ? ClusterStatus.CONNECTED : ClusterStatus.DISCONNECTED,
                healthy
            ));
        }

        return reports;
    }

    /**
     * Get replication lag for all streams.
     */
    public Map<String, Long> getReplicationLag() {
        Map<String, Long> lags = new HashMap<>();
        for (Map.Entry<String, ReplicationStream> entry : replicationStreams.entrySet()) {
            lags.put(entry.getKey(), entry.getValue().getLag());
        }
        return lags;
    }

    @Override
    public void close() {
        replicationStreams.values().forEach(ReplicationStream::stop);
        clusters.values().forEach(c -> c.adminClient().close());
        executor.shutdown();
    }

    public enum ClusterRole {
        PRIMARY,
        SECONDARY,
        STANDBY
    }

    public enum ClusterStatus {
        CONNECTED,
        DISCONNECTED,
        DEGRADED
    }

    public enum ReplicationMode {
        SYNC,
        ASYNC,
        SEMI_SYNC
    }

    public record ClusterConnection(
        String name,
        KafkaSdkConfig config,
        KafkaAdminClient adminClient,
        ClusterRole role,
        ClusterStatus status
    ) {
        ClusterConnection withRole(ClusterRole newRole) {
            return new ClusterConnection(name, config, adminClient, newRole, status);
        }

        ClusterConnection withStatus(ClusterStatus newStatus) {
            return new ClusterConnection(name, config, adminClient, role, newStatus);
        }
    }

    public record ClusterHealthReport(
        String clusterName,
        ClusterRole role,
        ClusterStatus status,
        boolean healthy
    ) {}

    /**
     * Replication stream between two clusters.
     */
    private static class ReplicationStream {
        private final String id;
        private final ClusterConnection source;
        private final ClusterConnection target;
        private final List<String> topics;
        private final ReplicationMode mode;
        private final AtomicLong messagesReplicated;
        private final AtomicLong lag;
        private volatile boolean running;

        private SmartConsumer<String, String> consumer;
        private SmartProducer<String, String> producer;

        ReplicationStream(String id, ClusterConnection source, ClusterConnection target,
                         List<String> topics, ReplicationMode mode) {
            this.id = id;
            this.source = source;
            this.target = target;
            this.topics = topics;
            this.mode = mode;
            this.messagesReplicated = new AtomicLong(0);
            this.lag = new AtomicLong(0);
            this.running = false;
        }

        void start() {
            running = true;

            // Create consumer from source cluster
            consumer = ConsumerFactory.createStringConsumer(source.config());

            // Create producer to target cluster
            producer = ProducerFactory.createStringProducer(target.config());

            // Subscribe to topics
            consumer.subscribe(topics, this::replicateRecord);
            consumer.start();

            logger.info("Replication stream {} started", id);
        }

        void stop() {
            running = false;
            if (consumer != null) {
                consumer.close();
            }
            if (producer != null) {
                producer.close();
            }
            logger.info("Replication stream {} stopped", id);
        }

        private void replicateRecord(ConsumerRecord<String, String> record) {
            if (!running) {
                return;
            }

            try {
                if (mode == ReplicationMode.SYNC) {
                    // Synchronous replication
                    producer.send(record.topic(), record.key(), record.value())
                        .get(30, TimeUnit.SECONDS);
                } else {
                    // Async replication
                    producer.send(record.topic(), record.key(), record.value());
                }

                messagesReplicated.incrementAndGet();

                // Update lag estimate
                long currentLag = System.currentTimeMillis() - record.timestamp();
                lag.set(currentLag);

            } catch (Exception e) {
                logger.error("Failed to replicate record", e);
            }
        }

        long getLag() {
            return lag.get();
        }

        ClusterConnection source() {
            return source;
        }

        ClusterConnection target() {
            return target;
        }
    }
}
