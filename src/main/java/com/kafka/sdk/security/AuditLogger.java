package com.kafka.sdk.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Audit logging for security and compliance.
 */
public class AuditLogger {

    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");
    private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);

    /**
     * Log a produce event.
     */
    public static void logProduce(String topic, String key, String correlationId, String userId) {
        AuditEvent event = new AuditEvent.Builder()
            .eventType(EventType.PRODUCE)
            .topic(topic)
            .key(key)
            .correlationId(correlationId)
            .userId(userId)
            .build();

        log(event);
    }

    /**
     * Log a consume event.
     */
    public static void logConsume(String topic, int partition, long offset, String groupId, String userId) {
        AuditEvent event = new AuditEvent.Builder()
            .eventType(EventType.CONSUME)
            .topic(topic)
            .partition(partition)
            .offset(offset)
            .groupId(groupId)
            .userId(userId)
            .build();

        log(event);
    }

    /**
     * Log an admin operation.
     */
    public static void logAdminOperation(String operation, String resource, String userId, boolean success) {
        AuditEvent event = new AuditEvent.Builder()
            .eventType(EventType.ADMIN)
            .operation(operation)
            .resource(resource)
            .userId(userId)
            .success(success)
            .build();

        log(event);
    }

    /**
     * Log an authentication event.
     */
    public static void logAuthentication(String userId, String mechanism, boolean success, String remoteAddress) {
        AuditEvent event = new AuditEvent.Builder()
            .eventType(EventType.AUTHENTICATION)
            .userId(userId)
            .operation(mechanism)
            .success(success)
            .remoteAddress(remoteAddress)
            .build();

        log(event);
    }

    /**
     * Log an authorization event.
     */
    public static void logAuthorization(String userId, String resource, String action, boolean allowed) {
        AuditEvent event = new AuditEvent.Builder()
            .eventType(EventType.AUTHORIZATION)
            .userId(userId)
            .resource(resource)
            .operation(action)
            .success(allowed)
            .build();

        log(event);
    }

    /**
     * Log a custom audit event.
     */
    public static void log(AuditEvent event) {
        String auditMessage = formatAuditEvent(event);
        auditLogger.info(auditMessage);
    }

    private static String formatAuditEvent(AuditEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append("AUDIT|");
        sb.append("timestamp=").append(event.timestamp).append("|");
        sb.append("eventType=").append(event.eventType).append("|");

        if (event.userId != null) {
            sb.append("userId=").append(event.userId).append("|");
        }
        if (event.topic != null) {
            sb.append("topic=").append(event.topic).append("|");
        }
        if (event.partition != null) {
            sb.append("partition=").append(event.partition).append("|");
        }
        if (event.offset != null) {
            sb.append("offset=").append(event.offset).append("|");
        }
        if (event.key != null) {
            sb.append("key=").append(event.key).append("|");
        }
        if (event.groupId != null) {
            sb.append("groupId=").append(event.groupId).append("|");
        }
        if (event.correlationId != null) {
            sb.append("correlationId=").append(event.correlationId).append("|");
        }
        if (event.operation != null) {
            sb.append("operation=").append(event.operation).append("|");
        }
        if (event.resource != null) {
            sb.append("resource=").append(event.resource).append("|");
        }
        if (event.remoteAddress != null) {
            sb.append("remoteAddress=").append(event.remoteAddress).append("|");
        }
        sb.append("success=").append(event.success);

        for (Map.Entry<String, String> entry : event.additionalData.entrySet()) {
            sb.append("|").append(entry.getKey()).append("=").append(entry.getValue());
        }

        return sb.toString();
    }

    public enum EventType {
        PRODUCE,
        CONSUME,
        ADMIN,
        AUTHENTICATION,
        AUTHORIZATION,
        CUSTOM
    }

    public static class AuditEvent {
        private final Instant timestamp;
        private final EventType eventType;
        private final String userId;
        private final String topic;
        private final Integer partition;
        private final Long offset;
        private final String key;
        private final String groupId;
        private final String correlationId;
        private final String operation;
        private final String resource;
        private final String remoteAddress;
        private final boolean success;
        private final Map<String, String> additionalData;

        private AuditEvent(Builder builder) {
            this.timestamp = Instant.now();
            this.eventType = builder.eventType;
            this.userId = builder.userId;
            this.topic = builder.topic;
            this.partition = builder.partition;
            this.offset = builder.offset;
            this.key = builder.key;
            this.groupId = builder.groupId;
            this.correlationId = builder.correlationId;
            this.operation = builder.operation;
            this.resource = builder.resource;
            this.remoteAddress = builder.remoteAddress;
            this.success = builder.success;
            this.additionalData = new HashMap<>(builder.additionalData);
        }

        public static class Builder {
            private EventType eventType = EventType.CUSTOM;
            private String userId;
            private String topic;
            private Integer partition;
            private Long offset;
            private String key;
            private String groupId;
            private String correlationId;
            private String operation;
            private String resource;
            private String remoteAddress;
            private boolean success = true;
            private final Map<String, String> additionalData = new HashMap<>();

            public Builder eventType(EventType eventType) {
                this.eventType = eventType;
                return this;
            }

            public Builder userId(String userId) {
                this.userId = userId;
                return this;
            }

            public Builder topic(String topic) {
                this.topic = topic;
                return this;
            }

            public Builder partition(int partition) {
                this.partition = partition;
                return this;
            }

            public Builder offset(long offset) {
                this.offset = offset;
                return this;
            }

            public Builder key(String key) {
                this.key = key;
                return this;
            }

            public Builder groupId(String groupId) {
                this.groupId = groupId;
                return this;
            }

            public Builder correlationId(String correlationId) {
                this.correlationId = correlationId;
                return this;
            }

            public Builder operation(String operation) {
                this.operation = operation;
                return this;
            }

            public Builder resource(String resource) {
                this.resource = resource;
                return this;
            }

            public Builder remoteAddress(String remoteAddress) {
                this.remoteAddress = remoteAddress;
                return this;
            }

            public Builder success(boolean success) {
                this.success = success;
                return this;
            }

            public Builder addData(String key, String value) {
                this.additionalData.put(key, value);
                return this;
            }

            public AuditEvent build() {
                return new AuditEvent(this);
            }
        }
    }
}
