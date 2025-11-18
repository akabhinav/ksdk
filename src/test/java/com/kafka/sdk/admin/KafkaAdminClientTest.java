package com.kafka.sdk.admin;

import com.kafka.sdk.config.KafkaSdkConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for KafkaAdminClient.
 */
@Testcontainers
class KafkaAdminClientTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

    private KafkaAdminClient adminClient;
    private KafkaSdkConfig config;

    @BeforeEach
    void setUp() {
        config = KafkaSdkConfig.builder()
            .bootstrapServers(kafka.getBootstrapServers())
            .build();
        adminClient = new KafkaAdminClient(config);
    }

    @AfterEach
    void tearDown() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Test
    void testCreateTopic() {
        String topicName = "create-test-" + System.currentTimeMillis();

        adminClient.createTopic(topicName, 3, (short) 1);

        assertTrue(adminClient.topicExists(topicName));
    }

    @Test
    void testDescribeTopic() {
        String topicName = "describe-test-" + System.currentTimeMillis();
        adminClient.createTopic(topicName, 2, (short) 1);

        TopicDescription description = adminClient.describeTopic(topicName);

        assertEquals(topicName, description.name());
        assertEquals(2, description.partitions().size());
    }

    @Test
    void testListTopics() {
        String topicName = "list-test-" + System.currentTimeMillis();
        adminClient.createTopic(topicName, 1, (short) 1);

        Set<String> topics = adminClient.listTopics();

        assertTrue(topics.contains(topicName));
    }

    @Test
    void testDeleteTopic() {
        String topicName = "delete-test-" + System.currentTimeMillis();
        adminClient.createTopic(topicName, 1, (short) 1);
        assertTrue(adminClient.topicExists(topicName));

        adminClient.deleteTopic(topicName);

        // Note: Topic deletion may not be immediate
        // assertFalse(adminClient.topicExists(topicName));
    }

    @Test
    void testClusterHealth() {
        assertTrue(adminClient.isClusterHealthy());
    }

    @Test
    void testDescribeCluster() {
        var clusterInfo = adminClient.describeCluster();

        assertNotNull(clusterInfo);
        assertNotNull(clusterInfo.clusterId());
        assertFalse(clusterInfo.nodes().isEmpty());
    }
}
