package com.kafka.sdk.producer;

import com.kafka.sdk.config.KafkaSdkConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SmartProducer.
 */
@Testcontainers
class SmartProducerTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

    private SmartProducer<String, String> producer;
    private KafkaSdkConfig config;

    @BeforeEach
    void setUp() {
        config = KafkaSdkConfig.builder()
            .bootstrapServers(kafka.getBootstrapServers())
            .build();
        producer = ProducerFactory.createStringProducer(config);
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testSendMessage() throws Exception {
        String topic = "test-topic";
        String key = "test-key";
        String value = "test-value";

        CompletableFuture<RecordMetadata> future = producer.send(topic, key, value);
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);

        assertNotNull(metadata);
        assertEquals(topic, metadata.topic());
        assertTrue(metadata.offset() >= 0);
    }

    @Test
    void testSendBatch() throws Exception {
        String topic = "batch-test-topic";
        var values = java.util.List.of("value1", "value2", "value3");

        var futures = producer.sendBatch(topic, values);

        assertEquals(3, futures.size());
        for (var future : futures) {
            RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
            assertNotNull(metadata);
            assertEquals(topic, metadata.topic());
        }
    }

    @Test
    void testProducerMetrics() {
        ProducerMetrics metrics = producer.getMetrics();

        assertNotNull(metrics);
        assertEquals(0, metrics.getMessagesSent());
        assertEquals(0, metrics.getMessagesSucceeded());
        assertEquals(0, metrics.getMessagesFailed());
    }

    @Test
    void testProducerHealth() {
        assertTrue(producer.isHealthy());
    }

    @Test
    void testInterceptor() throws Exception {
        String topic = "interceptor-test";
        String value = "test-value";

        StringBuilder intercepted = new StringBuilder();
        producer.addInterceptor(record -> {
            intercepted.append("intercepted");
            return record;
        });

        producer.send(topic, value).get(10, TimeUnit.SECONDS);

        assertEquals("intercepted", intercepted.toString());
    }
}
