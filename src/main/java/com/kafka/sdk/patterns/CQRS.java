package com.kafka.sdk.patterns;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.consumer.ConsumerFactory;
import com.kafka.sdk.consumer.SmartConsumer;
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
 * Command Query Responsibility Segregation (CQRS) implementation.
 * Separates command handling from query processing with optimized read models.
 */
public class CQRS implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(CQRS.class);

    private final SmartProducer<String, String> commandProducer;
    private final SmartConsumer<String, String> commandConsumer;
    private final SmartProducer<String, String> eventProducer;
    private final ObjectMapper objectMapper;
    private final Map<String, CommandHandler<?>> commandHandlers;
    private final Map<String, List<Projector>> projectors;
    private final String commandTopic;
    private final String eventTopic;

    public CQRS(KafkaSdkConfig config, String commandTopic, String eventTopic) {
        this.commandProducer = ProducerFactory.createStringProducer(config);
        this.commandConsumer = ConsumerFactory.createStringConsumer(config);
        this.eventProducer = ProducerFactory.createStringProducer(config);
        this.objectMapper = new ObjectMapper();
        this.commandHandlers = new ConcurrentHashMap<>();
        this.projectors = new ConcurrentHashMap<>();
        this.commandTopic = commandTopic;
        this.eventTopic = eventTopic;
    }

    /**
     * Register a command handler.
     */
    public <C> void registerCommandHandler(String commandType, CommandHandler<C> handler) {
        commandHandlers.put(commandType, handler);
        logger.info("Registered command handler for: {}", commandType);
    }

    /**
     * Register a projector for an event type.
     */
    public void registerProjector(String eventType, Projector projector) {
        projectors.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>()).add(projector);
        logger.info("Registered projector for event type: {}", eventType);
    }

    /**
     * Send a command.
     */
    public CompletableFuture<CommandResult> sendCommand(Command command) {
        CompletableFuture<CommandResult> future = new CompletableFuture<>();

        try {
            CommandEnvelope envelope = new CommandEnvelope(
                UUID.randomUUID().toString(),
                command.commandType(),
                command.aggregateId(),
                command.payload(),
                Instant.now(),
                command.metadata()
            );

            String payload = objectMapper.writeValueAsString(envelope);
            RecordHeaders headers = new RecordHeaders();
            headers.add("x-command-id", envelope.commandId().getBytes());
            headers.add("x-command-type", command.commandType().getBytes());

            commandProducer.send(commandTopic, command.aggregateId(), payload, headers)
                .thenAccept(metadata -> {
                    future.complete(new CommandResult(
                        envelope.commandId(),
                        true,
                        null,
                        metadata.offset()
                    ));
                })
                .exceptionally(e -> {
                    future.complete(new CommandResult(
                        envelope.commandId(),
                        false,
                        e.getMessage(),
                        -1
                    ));
                    return null;
                });

        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Start the command processor.
     */
    public void startCommandProcessor() {
        commandConsumer.subscribe(commandTopic, this::processCommand);
        commandConsumer.start();
        logger.info("Command processor started");
    }

    @SuppressWarnings("unchecked")
    private void processCommand(ConsumerRecord<String, String> record) {
        try {
            CommandEnvelope envelope = objectMapper.readValue(
                record.value(), CommandEnvelope.class);

            CommandHandler<Object> handler =
                (CommandHandler<Object>) commandHandlers.get(envelope.commandType());

            if (handler == null) {
                logger.warn("No handler for command type: {}", envelope.commandType());
                return;
            }

            // Execute command and get events
            List<Event> events = handler.handle(envelope);

            // Publish events
            for (Event event : events) {
                publishEvent(event, envelope.commandId());
            }

            logger.debug("Processed command {} -> {} events",
                envelope.commandId(), events.size());

        } catch (Exception e) {
            logger.error("Error processing command", e);
        }
    }

    private void publishEvent(Event event, String commandId) {
        try {
            EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID().toString(),
                event.eventType(),
                event.aggregateId(),
                event.payload(),
                Instant.now(),
                commandId,
                event.metadata()
            );

            String payload = objectMapper.writeValueAsString(envelope);
            RecordHeaders headers = new RecordHeaders();
            headers.add("x-event-id", envelope.eventId().getBytes());
            headers.add("x-event-type", event.eventType().getBytes());

            eventProducer.send(eventTopic, event.aggregateId(), payload, headers)
                .thenAccept(metadata -> {
                    // Trigger projectors
                    List<Projector> eventProjectors = projectors.get(event.eventType());
                    if (eventProjectors != null) {
                        for (Projector projector : eventProjectors) {
                            try {
                                projector.project(envelope);
                            } catch (Exception e) {
                                logger.error("Projector failed for event {}", envelope.eventId(), e);
                            }
                        }
                    }
                });

        } catch (Exception e) {
            logger.error("Failed to publish event", e);
        }
    }

    @Override
    public void close() {
        commandConsumer.close();
        commandProducer.close();
        eventProducer.close();
    }

    /**
     * Command interface.
     */
    public record Command(
        String commandType,
        String aggregateId,
        Map<String, Object> payload,
        Map<String, String> metadata
    ) {
        public Command(String commandType, String aggregateId, Map<String, Object> payload) {
            this(commandType, aggregateId, payload, Map.of());
        }
    }

    /**
     * Command envelope for transport.
     */
    public record CommandEnvelope(
        String commandId,
        String commandType,
        String aggregateId,
        Map<String, Object> payload,
        Instant timestamp,
        Map<String, String> metadata
    ) {}

    /**
     * Event interface.
     */
    public record Event(
        String eventType,
        String aggregateId,
        Map<String, Object> payload,
        Map<String, String> metadata
    ) {
        public Event(String eventType, String aggregateId, Map<String, Object> payload) {
            this(eventType, aggregateId, payload, Map.of());
        }
    }

    /**
     * Event envelope for transport.
     */
    public record EventEnvelope(
        String eventId,
        String eventType,
        String aggregateId,
        Map<String, Object> payload,
        Instant timestamp,
        String causationId,
        Map<String, String> metadata
    ) {}

    /**
     * Command result.
     */
    public record CommandResult(
        String commandId,
        boolean success,
        String error,
        long offset
    ) {}

    /**
     * Command handler interface.
     */
    @FunctionalInterface
    public interface CommandHandler<C> {
        List<Event> handle(CommandEnvelope command);
    }

    /**
     * Projector interface for building read models.
     */
    @FunctionalInterface
    public interface Projector {
        void project(EventEnvelope event);
    }

    /**
     * Read model store interface.
     */
    public interface ReadModelStore<T> {
        void save(String id, T model);
        Optional<T> findById(String id);
        List<T> findAll();
        void delete(String id);
    }

    /**
     * In-memory read model store implementation.
     */
    public static class InMemoryReadModelStore<T> implements ReadModelStore<T> {
        private final Map<String, T> store = new ConcurrentHashMap<>();

        @Override
        public void save(String id, T model) {
            store.put(id, model);
        }

        @Override
        public Optional<T> findById(String id) {
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public List<T> findAll() {
            return new ArrayList<>(store.values());
        }

        @Override
        public void delete(String id) {
            store.remove(id);
        }
    }
}
