package com.kafka.sdk.patterns;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.sdk.producer.SmartProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Transactional Outbox pattern implementation for reliable message publishing.
 * Ensures exactly-once delivery by storing messages in a database table
 * before publishing to Kafka.
 */
public class TransactionalOutbox implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalOutbox.class);

    private final DataSource dataSource;
    private final SmartProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final String tableName;
    private final int batchSize;
    private final long pollingIntervalMs;

    private volatile boolean running = false;

    public TransactionalOutbox(DataSource dataSource, SmartProducer<String, String> producer) {
        this(dataSource, producer, "outbox_events", 100, 1000);
    }

    public TransactionalOutbox(
            DataSource dataSource,
            SmartProducer<String, String> producer,
            String tableName,
            int batchSize,
            long pollingIntervalMs) {
        this.dataSource = dataSource;
        this.producer = producer;
        this.objectMapper = new ObjectMapper();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.pollingIntervalMs = pollingIntervalMs;
    }

    /**
     * Initialize the outbox table.
     */
    public void initialize() throws SQLException {
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(36) PRIMARY KEY,
                aggregate_type VARCHAR(255) NOT NULL,
                aggregate_id VARCHAR(255) NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                topic VARCHAR(255) NOT NULL,
                partition_key VARCHAR(255),
                payload TEXT NOT NULL,
                headers TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                published_at TIMESTAMP,
                status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                retry_count INT DEFAULT 0,
                last_error TEXT
            )
            """.formatted(tableName);

        String createIndexSql = """
            CREATE INDEX IF NOT EXISTS idx_%s_status_created
            ON %s (status, created_at)
            """.formatted(tableName, tableName);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
            stmt.execute(createIndexSql);
            logger.info("Outbox table '{}' initialized", tableName);
        }
    }

    /**
     * Save an event to the outbox within the current transaction.
     */
    public void save(Connection conn, OutboxEvent event) throws SQLException {
        String sql = """
            INSERT INTO %s (id, aggregate_type, aggregate_id, event_type, topic,
                           partition_key, payload, headers, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            """.formatted(tableName);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, event.id() != null ? event.id() : UUID.randomUUID().toString());
            ps.setString(2, event.aggregateType());
            ps.setString(3, event.aggregateId());
            ps.setString(4, event.eventType());
            ps.setString(5, event.topic());
            ps.setString(6, event.partitionKey());
            ps.setString(7, event.payload());
            ps.setString(8, event.headers() != null ? objectMapper.writeValueAsString(event.headers()) : null);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new SQLException("Failed to save outbox event", e);
        }
    }

    /**
     * Start the outbox publisher.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        scheduler.scheduleWithFixedDelay(
            this::publishPendingEvents,
            0,
            pollingIntervalMs,
            TimeUnit.MILLISECONDS
        );
        logger.info("Transactional outbox publisher started");
    }

    /**
     * Stop the outbox publisher.
     */
    public void stop() {
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Transactional outbox publisher stopped");
    }

    /**
     * Publish pending events from the outbox.
     */
    private void publishPendingEvents() {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Select and lock pending events
            String selectSql = """
                SELECT id, topic, partition_key, payload, headers, retry_count
                FROM %s
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT %d
                FOR UPDATE SKIP LOCKED
                """.formatted(tableName, batchSize);

            List<PendingEvent> events = new ArrayList<>();

            try (PreparedStatement ps = conn.prepareStatement(selectSql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    events.add(new PendingEvent(
                        rs.getString("id"),
                        rs.getString("topic"),
                        rs.getString("partition_key"),
                        rs.getString("payload"),
                        rs.getString("headers"),
                        rs.getInt("retry_count")
                    ));
                }
            }

            if (events.isEmpty()) {
                conn.rollback();
                return;
            }

            // Publish events
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            Map<String, Boolean> results = new ConcurrentHashMap<>();

            for (PendingEvent event : events) {
                CompletableFuture<Void> future = producer.send(
                    event.topic(),
                    event.partitionKey(),
                    event.payload()
                ).thenAccept(metadata -> {
                    results.put(event.id(), true);
                    logger.debug("Published event {} to topic {} partition {} offset {}",
                        event.id(), metadata.topic(), metadata.partition(), metadata.offset());
                }).exceptionally(e -> {
                    results.put(event.id(), false);
                    logger.error("Failed to publish event {}: {}", event.id(), e.getMessage());
                    return null;
                });
                futures.add(future);
            }

            // Wait for all publishes
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);

            // Update event statuses
            String updateSuccessSql = """
                UPDATE %s SET status = 'PUBLISHED', published_at = ? WHERE id = ?
                """.formatted(tableName);
            String updateFailureSql = """
                UPDATE %s SET status = CASE WHEN retry_count >= 3 THEN 'FAILED' ELSE 'PENDING' END,
                             retry_count = retry_count + 1,
                             last_error = ?
                WHERE id = ?
                """.formatted(tableName);

            try (PreparedStatement successPs = conn.prepareStatement(updateSuccessSql);
                 PreparedStatement failurePs = conn.prepareStatement(updateFailureSql)) {

                for (PendingEvent event : events) {
                    Boolean success = results.get(event.id());
                    if (Boolean.TRUE.equals(success)) {
                        successPs.setTimestamp(1, Timestamp.from(Instant.now()));
                        successPs.setString(2, event.id());
                        successPs.addBatch();
                    } else {
                        failurePs.setString(1, "Publish failed");
                        failurePs.setString(2, event.id());
                        failurePs.addBatch();
                    }
                }

                successPs.executeBatch();
                failurePs.executeBatch();
            }

            conn.commit();

            int published = (int) results.values().stream().filter(b -> b).count();
            logger.info("Published {}/{} outbox events", published, events.size());

        } catch (Exception e) {
            logger.error("Error publishing outbox events", e);
        }
    }

    /**
     * Cleanup old published events.
     */
    public int cleanup(int daysOld) throws SQLException {
        String sql = """
            DELETE FROM %s
            WHERE status = 'PUBLISHED'
            AND published_at < ?
            """.formatted(tableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(
                Instant.now().minus(java.time.Duration.ofDays(daysOld))));
            int deleted = ps.executeUpdate();
            logger.info("Cleaned up {} old outbox events", deleted);
            return deleted;
        }
    }

    /**
     * Retry failed events.
     */
    public int retryFailed() throws SQLException {
        String sql = """
            UPDATE %s SET status = 'PENDING', retry_count = 0 WHERE status = 'FAILED'
            """.formatted(tableName);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            int updated = stmt.executeUpdate(sql);
            logger.info("Reset {} failed events for retry", updated);
            return updated;
        }
    }

    @Override
    public void close() {
        stop();
    }

    public record OutboxEvent(
        String id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String topic,
        String partitionKey,
        String payload,
        Map<String, String> headers
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String id;
            private String aggregateType;
            private String aggregateId;
            private String eventType;
            private String topic;
            private String partitionKey;
            private String payload;
            private Map<String, String> headers;

            public Builder id(String id) {
                this.id = id;
                return this;
            }

            public Builder aggregateType(String aggregateType) {
                this.aggregateType = aggregateType;
                return this;
            }

            public Builder aggregateId(String aggregateId) {
                this.aggregateId = aggregateId;
                return this;
            }

            public Builder eventType(String eventType) {
                this.eventType = eventType;
                return this;
            }

            public Builder topic(String topic) {
                this.topic = topic;
                return this;
            }

            public Builder partitionKey(String partitionKey) {
                this.partitionKey = partitionKey;
                return this;
            }

            public Builder payload(String payload) {
                this.payload = payload;
                return this;
            }

            public Builder headers(Map<String, String> headers) {
                this.headers = headers;
                return this;
            }

            public OutboxEvent build() {
                return new OutboxEvent(id, aggregateType, aggregateId, eventType,
                    topic, partitionKey, payload, headers);
            }
        }
    }

    private record PendingEvent(
        String id,
        String topic,
        String partitionKey,
        String payload,
        String headers,
        int retryCount
    ) {}
}
