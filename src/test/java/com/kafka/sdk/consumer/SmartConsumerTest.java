package com.kafka.sdk.consumer;

import com.kafka.sdk.config.ConsumerConfig;
import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.producer.ProducerFactory;
import com.kafka.sdk.producer.SmartProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SmartConsumer.
 */
@Testcontainers
class SmartConsumerTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

    private SmartProducer<String, String> producer;
    private SmartConsumer<String, String> consumer;
    private KafkaSdkConfig config;

    @BeforeEach
    void setUp() {
        config = KafkaSdkConfig.builder()
            .bootstrapServers(kafka.getBootstrapServers())
            .consumerConfig(ConsumerConfig.builder()
                .groupId("test-group-" + System.currentTimeMillis())
                .autoOffsetReset(ConsumerConfig.AutoOffsetReset.EARLIEST)
                .build())
            .build();

        producer = ProducerFactory.createStringProducer(config);
        consumer = ConsumerFactory.createStringConsumer(config);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testConsumeMessage() throws Exception {
        String topic = "consume-test-" + System.currentTimeMillis();
        String expectedValue = "test-value";

        // Send message
        producer.send(topic, "key", expectedValue).get(10, TimeUnit.SECONDS);
        producer.flush();

        // Consume message
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedValue = new AtomicReference<>();

        consumer.subscribe(topic, record -> {
            receivedValue.set(record.value());
            latch.countDown();
        });
        consumer.start();

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Message not received within timeout");
        assertEquals(expectedValue, receivedValue.get());
    }

    @Test
    void testConsumerMetrics() {
        ConsumerMetrics metrics = consumer.getMetrics();

        assertNotNull(metrics);
        assertEquals(0, metrics.getMessagesConsumed());
        assertEquals(0, metrics.getMessagesProcessed());
        assertEquals(0, metrics.getMessagesFailed());
    }

    @Test
    void testConsumerFilter() throws Exception {
        String topic = "filter-test-" + System.currentTimeMillis();

        // Send messages
        producer.send(topic, "key1", "accept-this").get(10, TimeUnit.SECONDS);
        producer.send(topic, "key2", "reject-this").get(10, TimeUnit.SECONDS);
        producer.flush();

        // Consume with filter
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedValue = new AtomicReference<>();

        consumer.addFilter(record -> record.value().startsWith("accept"));
        consumer.subscribe(topic, record -> {
            receivedValue.set(record.value());
            latch.countDown();
        });
        consumer.start();

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals("accept-this", receivedValue.get());
    }

    @Test
    void testConsumerGroupId() {
        assertNotNull(consumer.getGroupId());
        assertTrue(consumer.getGroupId().startsWith("test-group-"));
    }
}
