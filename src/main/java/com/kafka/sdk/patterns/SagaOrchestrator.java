package com.kafka.sdk.patterns;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.sdk.consumer.ConsumerFactory;
import com.kafka.sdk.consumer.SmartConsumer;
import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.producer.ProducerFactory;
import com.kafka.sdk.producer.SmartProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Saga pattern orchestrator for distributed transactions.
 * Coordinates multi-step business transactions with compensation on failure.
 */
public class SagaOrchestrator implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SmartProducer<String, String> producer;
    private final SmartConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final Map<String, SagaDefinition> sagaDefinitions;
    private final Map<String, SagaInstance> activeSagas;
    private final String replyTopic;

    public SagaOrchestrator(KafkaSdkConfig config, String replyTopic) {
        this.producer = ProducerFactory.createStringProducer(config);
        this.consumer = ConsumerFactory.createStringConsumer(config);
        this.objectMapper = new ObjectMapper();
        this.sagaDefinitions = new ConcurrentHashMap<>();
        this.activeSagas = new ConcurrentHashMap<>();
        this.replyTopic = replyTopic;
    }

    /**
     * Register a saga definition.
     */
    public void registerSaga(SagaDefinition definition) {
        sagaDefinitions.put(definition.name(), definition);
        logger.info("Registered saga: {}", definition.name());
    }

    /**
     * Start the orchestrator.
     */
    public void start() {
        consumer.subscribe(replyTopic, this::handleReply);
        consumer.start();
        logger.info("Saga orchestrator started");
    }

    /**
     * Execute a saga.
     */
    public CompletableFuture<SagaResult> executeSaga(String sagaName, Map<String, Object> context) {
        SagaDefinition definition = sagaDefinitions.get(sagaName);
        if (definition == null) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown saga: " + sagaName));
        }

        String sagaId = UUID.randomUUID().toString();
        SagaInstance instance = new SagaInstance(
            sagaId,
            sagaName,
            definition,
            new HashMap<>(context),
            new CompletableFuture<>()
        );

        activeSagas.put(sagaId, instance);
        logger.info("Starting saga {} with ID {}", sagaName, sagaId);

        // Execute first step
        executeStep(instance, 0);

        return instance.result();
    }

    private void executeStep(SagaInstance instance, int stepIndex) {
        if (stepIndex >= instance.definition().steps().size()) {
            // Saga completed successfully
            completeSaga(instance, true, null);
            return;
        }

        SagaStep step = instance.definition().steps().get(stepIndex);
        instance.setCurrentStep(stepIndex);

        try {
            // Build command
            String payload = objectMapper.writeValueAsString(Map.of(
                "sagaId", instance.id(),
                "step", step.name(),
                "command", step.command(),
                "data", instance.context()
            ));

            RecordHeaders headers = new RecordHeaders();
            headers.add("x-saga-id", instance.id().getBytes());
            headers.add("x-saga-step", String.valueOf(stepIndex).getBytes());
            headers.add("x-reply-topic", replyTopic.getBytes());

            // Send command to step's topic
            producer.send(step.topic(), instance.id(), payload, headers)
                .thenAccept(metadata -> {
                    logger.debug("Sent step {} command to {}", step.name(), step.topic());
                })
                .exceptionally(e -> {
                    logger.error("Failed to send step {} command", step.name(), e);
                    compensateSaga(instance, stepIndex - 1, e.getMessage());
                    return null;
                });

        } catch (Exception e) {
            logger.error("Error executing step {}", step.name(), e);
            compensateSaga(instance, stepIndex - 1, e.getMessage());
        }
    }

    private void handleReply(ConsumerRecord<String, String> record) {
        try {
            var header = record.headers().lastHeader("x-saga-id");
            if (header == null) {
                return;
            }

            String sagaId = new String(header.value());
            SagaInstance instance = activeSagas.get(sagaId);
            if (instance == null) {
                logger.warn("Received reply for unknown saga: {}", sagaId);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> reply = objectMapper.readValue(record.value(), Map.class);
            boolean success = (Boolean) reply.getOrDefault("success", false);
            String error = (String) reply.get("error");

            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) reply.get("data");
            if (data != null) {
                instance.context().putAll(data);
            }

            if (success) {
                // Move to next step
                executeStep(instance, instance.currentStep() + 1);
            } else {
                // Start compensation
                logger.warn("Step {} failed: {}", instance.currentStep(), error);
                compensateSaga(instance, instance.currentStep() - 1, error);
            }

        } catch (Exception e) {
            logger.error("Error handling saga reply", e);
        }
    }

    private void compensateSaga(SagaInstance instance, int fromStep, String error) {
        logger.info("Compensating saga {} from step {}", instance.id(), fromStep);

        // Execute compensations in reverse order
        for (int i = fromStep; i >= 0; i--) {
            SagaStep step = instance.definition().steps().get(i);
            if (step.compensationTopic() != null) {
                try {
                    String payload = objectMapper.writeValueAsString(Map.of(
                        "sagaId", instance.id(),
                        "step", step.name(),
                        "compensation", step.compensation(),
                        "data", instance.context()
                    ));

                    producer.send(step.compensationTopic(), instance.id(), payload)
                        .get(10, TimeUnit.SECONDS);

                    logger.debug("Sent compensation for step {}", step.name());
                } catch (Exception e) {
                    logger.error("Failed to send compensation for step {}", step.name(), e);
                }
            }
        }

        completeSaga(instance, false, error);
    }

    private void completeSaga(SagaInstance instance, boolean success, String error) {
        activeSagas.remove(instance.id());

        SagaResult result = new SagaResult(
            instance.id(),
            instance.sagaName(),
            success ? SagaStatus.COMPLETED : SagaStatus.COMPENSATED,
            instance.context(),
            error,
            Instant.now()
        );

        instance.result().complete(result);
        logger.info("Saga {} completed with status: {}", instance.id(),
            success ? "COMPLETED" : "COMPENSATED");
    }

    @Override
    public void close() {
        consumer.close();
        producer.close();
    }

    public record SagaDefinition(
        String name,
        List<SagaStep> steps
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String name;
            private final List<SagaStep> steps = new ArrayList<>();

            public Builder name(String name) {
                this.name = name;
                return this;
            }

            public Builder step(SagaStep step) {
                this.steps.add(step);
                return this;
            }

            public Builder step(String name, String topic, String command,
                              String compensationTopic, String compensation) {
                this.steps.add(new SagaStep(name, topic, command, compensationTopic, compensation));
                return this;
            }

            public SagaDefinition build() {
                return new SagaDefinition(name, List.copyOf(steps));
            }
        }
    }

    public record SagaStep(
        String name,
        String topic,
        String command,
        String compensationTopic,
        String compensation
    ) {}

    public record SagaResult(
        String sagaId,
        String sagaName,
        SagaStatus status,
        Map<String, Object> context,
        String error,
        Instant completedAt
    ) {}

    public enum SagaStatus {
        STARTED,
        IN_PROGRESS,
        COMPLETED,
        COMPENSATING,
        COMPENSATED,
        FAILED
    }

    private static class SagaInstance {
        private final String id;
        private final String sagaName;
        private final SagaDefinition definition;
        private final Map<String, Object> context;
        private final CompletableFuture<SagaResult> result;
        private volatile int currentStep;

        SagaInstance(String id, String sagaName, SagaDefinition definition,
                    Map<String, Object> context, CompletableFuture<SagaResult> result) {
            this.id = id;
            this.sagaName = sagaName;
            this.definition = definition;
            this.context = context;
            this.result = result;
            this.currentStep = 0;
        }

        String id() { return id; }
        String sagaName() { return sagaName; }
        SagaDefinition definition() { return definition; }
        Map<String, Object> context() { return context; }
        CompletableFuture<SagaResult> result() { return result; }
        int currentStep() { return currentStep; }
        void setCurrentStep(int step) { this.currentStep = step; }
    }
}
