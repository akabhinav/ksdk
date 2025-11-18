package com.kafka.sdk.resilience;

import com.kafka.sdk.admin.KafkaAdminClient;
import com.kafka.sdk.config.KafkaSdkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health check endpoints for Kafka connectivity.
 */
public class HealthCheck implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheck.class);

    private final KafkaAdminClient adminClient;
    private final AtomicReference<HealthStatus> lastStatus;
    private final AtomicReference<Instant> lastCheck;

    public HealthCheck(KafkaSdkConfig config) {
        this.adminClient = new KafkaAdminClient(config);
        this.lastStatus = new AtomicReference<>(HealthStatus.UNKNOWN);
        this.lastCheck = new AtomicReference<>(Instant.EPOCH);
    }

    /**
     * Readiness probe - can the application consume/produce?
     */
    public boolean isReady() {
        try {
            boolean healthy = adminClient.isClusterHealthy();
            lastStatus.set(healthy ? HealthStatus.HEALTHY : HealthStatus.UNHEALTHY);
            lastCheck.set(Instant.now());
            return healthy;
        } catch (Exception e) {
            logger.error("Readiness check failed", e);
            lastStatus.set(HealthStatus.UNHEALTHY);
            lastCheck.set(Instant.now());
            return false;
        }
    }

    /**
     * Liveness probe - is the connection active?
     */
    public boolean isAlive() {
        try {
            // Simple check - can we reach the cluster?
            adminClient.listTopics();
            return true;
        } catch (Exception e) {
            logger.error("Liveness check failed", e);
            return false;
        }
    }

    /**
     * Startup probe - has initialization completed?
     */
    public boolean isStarted() {
        // Check if we can successfully connect
        return isAlive();
    }

    /**
     * Get detailed health status.
     */
    public HealthReport getHealthReport() {
        boolean ready = isReady();
        boolean alive = isAlive();

        return new HealthReport(
            ready && alive ? HealthStatus.HEALTHY : HealthStatus.UNHEALTHY,
            ready,
            alive,
            lastCheck.get(),
            getClusterInfo()
        );
    }

    private String getClusterInfo() {
        try {
            var description = adminClient.describeCluster();
            return String.format("Cluster ID: %s, Controller: %s, Nodes: %d",
                description.clusterId(),
                description.controller().host(),
                description.nodes().size());
        } catch (Exception e) {
            return "Unable to retrieve cluster info: " + e.getMessage();
        }
    }

    public HealthStatus getLastStatus() {
        return lastStatus.get();
    }

    public Instant getLastCheckTime() {
        return lastCheck.get();
    }

    @Override
    public void close() {
        adminClient.close();
    }

    public enum HealthStatus {
        HEALTHY,
        UNHEALTHY,
        UNKNOWN
    }

    public record HealthReport(
        HealthStatus status,
        boolean ready,
        boolean alive,
        Instant lastCheck,
        String clusterInfo
    ) {}
}
