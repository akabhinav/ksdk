package com.kafka.sdk.admin;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.core.KafkaSdkException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Kafka administration client for cluster and topic management.
 */
public class KafkaAdminClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final AdminClient adminClient;
    private final KafkaSdkConfig config;
    private final int timeoutMs;

    public KafkaAdminClient(KafkaSdkConfig config) {
        this.config = config;
        this.timeoutMs = config.getAdminConfig().getDefaultApiTimeoutMs();
        Properties props = config.toAdminProperties();
        this.adminClient = AdminClient.create(props);
        logger.info("KafkaAdminClient initialized");
    }

    // Topic Management

    /**
     * Create a topic with default settings.
     */
    public void createTopic(String topicName, int partitions, short replicationFactor) {
        createTopic(topicName, partitions, replicationFactor, Collections.emptyMap());
    }

    /**
     * Create a topic with custom configuration.
     */
    public void createTopic(String topicName, int partitions, short replicationFactor, Map<String, String> configs) {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        newTopic.configs(configs);

        try {
            adminClient.createTopics(Collections.singletonList(newTopic))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
            logger.info("Created topic: {} with {} partitions", topicName, partitions);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                logger.warn("Topic already exists: {}", topicName);
            } else {
                throw new KafkaSdkException("Failed to create topic: " + topicName, e,
                    KafkaSdkException.ErrorCode.TOPIC_CREATION_FAILED);
            }
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to create topic: " + topicName, e,
                KafkaSdkException.ErrorCode.TOPIC_CREATION_FAILED);
        }
    }

    /**
     * Delete a topic.
     */
    public void deleteTopic(String topicName) {
        try {
            adminClient.deleteTopics(Collections.singletonList(topicName))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
            logger.info("Deleted topic: {}", topicName);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to delete topic: " + topicName, e,
                KafkaSdkException.ErrorCode.TOPIC_DELETION_FAILED);
        }
    }

    /**
     * List all topics.
     */
    public Set<String> listTopics() {
        try {
            return adminClient.listTopics()
                .names()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to list topics", e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Check if a topic exists.
     */
    public boolean topicExists(String topicName) {
        return listTopics().contains(topicName);
    }

    /**
     * Get topic description.
     */
    public TopicDescription describeTopic(String topicName) {
        try {
            return adminClient.describeTopics(Collections.singletonList(topicName))
                .allTopicNames()
                .get(timeoutMs, TimeUnit.MILLISECONDS)
                .get(topicName);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to describe topic: " + topicName, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Increase partition count for a topic.
     */
    public void increasePartitions(String topicName, int newPartitionCount) {
        try {
            Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
            newPartitionsMap.put(topicName, NewPartitions.increaseTo(newPartitionCount));
            adminClient.createPartitions(newPartitionsMap)
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
            logger.info("Increased partitions for topic {} to {}", topicName, newPartitionCount);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to increase partitions for topic: " + topicName, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Update topic configuration.
     */
    public void updateTopicConfig(String topicName, Map<String, String> configs) {
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Collection<AlterConfigOp> configOps = configs.entrySet().stream()
                .map(entry -> new AlterConfigOp(
                    new ConfigEntry(entry.getKey(), entry.getValue()),
                    AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());

            adminClient.incrementalAlterConfigs(Collections.singletonMap(resource, configOps))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
            logger.info("Updated configuration for topic: {}", topicName);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to update topic configuration: " + topicName, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    // Consumer Group Management

    /**
     * List consumer groups.
     */
    public Collection<ConsumerGroupListing> listConsumerGroups() {
        try {
            return adminClient.listConsumerGroups()
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to list consumer groups", e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Describe a consumer group.
     */
    public ConsumerGroupDescription describeConsumerGroup(String groupId) {
        try {
            return adminClient.describeConsumerGroups(Collections.singletonList(groupId))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS)
                .get(groupId);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to describe consumer group: " + groupId, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Delete a consumer group.
     */
    public void deleteConsumerGroup(String groupId) {
        try {
            adminClient.deleteConsumerGroups(Collections.singletonList(groupId))
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
            logger.info("Deleted consumer group: {}", groupId);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to delete consumer group: " + groupId, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Get consumer group offsets.
     */
    public Map<TopicPartition, OffsetAndMetadata> getConsumerGroupOffsets(String groupId) {
        try {
            return adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to get consumer group offsets: " + groupId, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Reset consumer group offsets to earliest.
     */
    public void resetOffsetsToEarliest(String groupId, Collection<TopicPartition> partitions) {
        try {
            Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                adminClient.listOffsets(offsetSpecs)
                    .all()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            Map<TopicPartition, OffsetAndMetadata> newOffsets = offsets.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new OffsetAndMetadata(e.getValue().offset())));

            adminClient.alterConsumerGroupOffsets(groupId, newOffsets)
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);

            logger.info("Reset offsets to earliest for group: {}", groupId);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to reset offsets for group: " + groupId, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Reset consumer group offsets to latest.
     */
    public void resetOffsetsToLatest(String groupId, Collection<TopicPartition> partitions) {
        try {
            Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                adminClient.listOffsets(offsetSpecs)
                    .all()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            Map<TopicPartition, OffsetAndMetadata> newOffsets = offsets.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new OffsetAndMetadata(e.getValue().offset())));

            adminClient.alterConsumerGroupOffsets(groupId, newOffsets)
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);

            logger.info("Reset offsets to latest for group: {}", groupId);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to reset offsets for group: " + groupId, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Reset consumer group offsets to timestamp.
     */
    public void resetOffsetsToTimestamp(String groupId, Collection<TopicPartition> partitions, long timestamp) {
        try {
            Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.forTimestamp(timestamp)));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                adminClient.listOffsets(offsetSpecs)
                    .all()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            Map<TopicPartition, OffsetAndMetadata> newOffsets = offsets.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new OffsetAndMetadata(e.getValue().offset())));

            adminClient.alterConsumerGroupOffsets(groupId, newOffsets)
                .all()
                .get(timeoutMs, TimeUnit.MILLISECONDS);

            logger.info("Reset offsets to timestamp {} for group: {}", timestamp, groupId);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to reset offsets for group: " + groupId, e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    // Cluster Management

    /**
     * Get cluster information.
     */
    public DescribeClusterResult.ClusterDescription describeCluster() {
        try {
            DescribeClusterResult result = adminClient.describeCluster();
            return new DescribeClusterResult.ClusterDescription(
                result.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS),
                result.controller().get(timeoutMs, TimeUnit.MILLISECONDS),
                result.nodes().get(timeoutMs, TimeUnit.MILLISECONDS)
            );
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to describe cluster", e,
                KafkaSdkException.ErrorCode.ADMIN_OPERATION_FAILED);
        }
    }

    /**
     * Check cluster health.
     */
    public boolean isClusterHealthy() {
        try {
            adminClient.describeCluster()
                .nodes()
                .get(5000, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            logger.warn("Cluster health check failed", e);
            return false;
        }
    }

    @Override
    public void close() {
        adminClient.close(Duration.ofSeconds(30));
        logger.info("KafkaAdminClient closed");
    }

    /**
     * Cluster description result.
     */
    public static class DescribeClusterResult {
        public record ClusterDescription(
            String clusterId,
            org.apache.kafka.common.Node controller,
            Collection<org.apache.kafka.common.Node> nodes
        ) {}
    }
}
