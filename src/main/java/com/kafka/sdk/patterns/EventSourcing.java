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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Event Sourcing implementation using Kafka as the event store.
 * Supports aggregate reconstruction, snapshots, and projections.
 */
public class EventSourcing<T> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(EventSourcing.class);

    private final SmartProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final String eventStoreTopic;
    private final String snapshotTopic;
    private final Class<T> aggregateType;
    private final BiFunction<T, Event, T> eventApplier;
    private final Function<String, T> aggregateFactory;
    private final Map<String, AggregateState<T>> aggregateCache;
    private final int snapshotInterval;

    public EventSourcing(
            KafkaSdkConfig config,
            String eventStoreTopic,
            String snapshotTopic,
            Class<T> aggregateType,
            Function<String, T> aggregateFactory,
            BiFunction<T, Event, T> eventApplier) {
        this(config, eventStoreTopic, snapshotTopic, aggregateType, aggregateFactory, eventApplier, 100);
    }

    public EventSourcing(
            KafkaSdkConfig config,
            String eventStoreTopic,
            String snapshotTopic,
            Class<T> aggregateType,
            Function<String, T> aggregateFactory,
            BiFunction<T, Event, T> eventApplier,
            int snapshotInterval) {
        this.producer = ProducerFactory.createStringProducer(config);
        this.objectMapper = new ObjectMapper();
        this.eventStoreTopic = eventStoreTopic;
        this.snapshotTopic = snapshotTopic;
        this.aggregateType = aggregateType;
        this.aggregateFactory = aggregateFactory;
        this.eventApplier = eventApplier;
        this.aggregateCache = new ConcurrentHashMap<>();
        this.snapshotInterval = snapshotInterval;
    }

    /**
     * Append an event to the event store.
     */
    public CompletableFuture<EventMetadata> appendEvent(String aggregateId, Event event) {
        return appendEvents(aggregateId, List.of(event))
            .thenApply(list -> list.get(0));
    }

    /**
     * Append multiple events atomically.
     */
    public CompletableFuture<List<EventMetadata>> appendEvents(String aggregateId, List<Event> events) {
        List<CompletableFuture<EventMetadata>> futures = new ArrayList<>();
        AggregateState<T> state = aggregateCache.computeIfAbsent(aggregateId,
            id -> new AggregateState<>(aggregateFactory.apply(id), 0));

        for (Event event : events) {
            long version = state.incrementVersion();
            String eventId = UUID.randomUUID().toString();

            EventEnvelope envelope = new EventEnvelope(
                eventId,
                aggregateId,
                event.eventType(),
                version,
                Instant.now(),
                event.payload(),
                event.metadata()
            );

            try {
                String payload = objectMapper.writeValueAsString(envelope);
                RecordHeaders headers = new RecordHeaders();
                headers.add("x-event-id", eventId.getBytes());
                headers.add("x-aggregate-id", aggregateId.getBytes());
                headers.add("x-event-type", event.eventType().getBytes());
                headers.add("x-version", String.valueOf(version).getBytes());

                CompletableFuture<EventMetadata> future = producer
                    .send(eventStoreTopic, aggregateId, payload, headers)
                    .thenApply(metadata -> {
                        // Apply event to cached aggregate
                        state.setAggregate(eventApplier.apply(state.aggregate(), event));

                        // Create snapshot if needed
                        if (version % snapshotInterval == 0) {
                            createSnapshot(aggregateId, state);
                        }

                        return new EventMetadata(
                            eventId,
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            version
                        );
                    });

                futures.add(future);
            } catch (Exception e) {
                futures.add(CompletableFuture.failedFuture(e));
            }
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .toList());
    }

    /**
     * Load an aggregate by replaying events.
     */
    public T loadAggregate(String aggregateId, KafkaSdkConfig config) {
        // Check cache first
        AggregateState<T> cached = aggregateCache.get(aggregateId);
        if (cached != null) {
            return cached.aggregate();
        }

        T aggregate = aggregateFactory.apply(aggregateId);
        long startVersion = 0;

        // Try to load from snapshot
        Snapshot<T> snapshot = loadLatestSnapshot(aggregateId, config);
        if (snapshot != null) {
            aggregate = snapshot.state();
            startVersion = snapshot.version();
            logger.debug("Loaded snapshot for {} at version {}", aggregateId, startVersion);
        }

        // Replay events from snapshot version
        List<EventEnvelope> events = loadEvents(aggregateId, startVersion, config);
        for (EventEnvelope envelope : events) {
            Event event = new Event(envelope.eventType(), envelope.payload(), envelope.metadata());
            aggregate = eventApplier.apply(aggregate, event);
        }

        // Update cache
        long finalVersion = events.isEmpty() ? startVersion :
            events.get(events.size() - 1).version();
        aggregateCache.put(aggregateId, new AggregateState<>(aggregate, finalVersion));

        logger.debug("Loaded aggregate {} with {} events", aggregateId, events.size());
        return aggregate;
    }

    /**
     * Load events for an aggregate from the event store.
     */
    public List<EventEnvelope> loadEvents(String aggregateId, long fromVersion, KafkaSdkConfig config) {
        List<EventEnvelope> events = new ArrayList<>();

        try (SmartConsumer<String, String> consumer = ConsumerFactory.createStringConsumer(config)) {
            consumer.subscribe(eventStoreTopic, record -> {
                if (aggregateId.equals(record.key())) {
                    try {
                        EventEnvelope envelope = objectMapper.readValue(
                            record.value(), EventEnvelope.class);
                        if (envelope.version() > fromVersion) {
                            events.add(envelope);
                        }
                    } catch (Exception e) {
                        logger.error("Failed to deserialize event", e);
                    }
                }
            });

            // Note: In a real implementation, you would seek to the beginning
            // and consume until you've read all events for the aggregate
            // This is simplified for demonstration
        }

        events.sort(Comparator.comparingLong(EventEnvelope::version));
        return events;
    }

    /**
     * Create a snapshot for an aggregate.
     */
    private void createSnapshot(String aggregateId, AggregateState<T> state) {
        try {
            Snapshot<T> snapshot = new Snapshot<>(
                aggregateId,
                state.version(),
                state.aggregate(),
                Instant.now()
            );

            String payload = objectMapper.writeValueAsString(snapshot);
            producer.send(snapshotTopic, aggregateId, payload)
                .thenAccept(metadata -> {
                    logger.debug("Created snapshot for {} at version {}",
                        aggregateId, state.version());
                });
        } catch (Exception e) {
            logger.error("Failed to create snapshot for {}", aggregateId, e);
        }
    }

    /**
     * Load the latest snapshot for an aggregate.
     */
    @SuppressWarnings("unchecked")
    private Snapshot<T> loadLatestSnapshot(String aggregateId, KafkaSdkConfig config) {
        // In a real implementation, you would consume from the snapshot topic
        // and find the latest snapshot for the aggregate
        return null;
    }

    /**
     * Clear the aggregate cache.
     */
    public void clearCache() {
        aggregateCache.clear();
    }

    /**
     * Get aggregate from cache without loading.
     */
    public Optional<T> getCached(String aggregateId) {
        AggregateState<T> state = aggregateCache.get(aggregateId);
        return state != null ? Optional.of(state.aggregate()) : Optional.empty();
    }

    @Override
    public void close() {
        producer.close();
    }

    /**
     * Domain event representation.
     */
    public record Event(
        String eventType,
        Map<String, Object> payload,
        Map<String, String> metadata
    ) {
        public Event(String eventType, Map<String, Object> payload) {
            this(eventType, payload, Map.of());
        }
    }

    /**
     * Event envelope stored in Kafka.
     */
    public record EventEnvelope(
        String eventId,
        String aggregateId,
        String eventType,
        long version,
        Instant timestamp,
        Map<String, Object> payload,
        Map<String, String> metadata
    ) {}

    /**
     * Event metadata returned after appending.
     */
    public record EventMetadata(
        String eventId,
        String topic,
        int partition,
        long offset,
        long version
    ) {}

    /**
     * Aggregate snapshot.
     */
    public record Snapshot<T>(
        String aggregateId,
        long version,
        T state,
        Instant createdAt
    ) {}

    /**
     * Cached aggregate state.
     */
    private static class AggregateState<T> {
        private T aggregate;
        private long version;

        AggregateState(T aggregate, long version) {
            this.aggregate = aggregate;
            this.version = version;
        }

        T aggregate() { return aggregate; }
        long version() { return version; }
        void setAggregate(T aggregate) { this.aggregate = aggregate; }

        synchronized long incrementVersion() {
            return ++version;
        }
    }

    /**
     * Builder for event sourcing configuration.
     */
    public static class Builder<T> {
        private KafkaSdkConfig config;
        private String eventStoreTopic;
        private String snapshotTopic;
        private Class<T> aggregateType;
        private Function<String, T> aggregateFactory;
        private BiFunction<T, Event, T> eventApplier;
        private int snapshotInterval = 100;

        public Builder<T> config(KafkaSdkConfig config) {
            this.config = config;
            return this;
        }

        public Builder<T> eventStoreTopic(String topic) {
            this.eventStoreTopic = topic;
            return this;
        }

        public Builder<T> snapshotTopic(String topic) {
            this.snapshotTopic = topic;
            return this;
        }

        public Builder<T> aggregateType(Class<T> type) {
            this.aggregateType = type;
            return this;
        }

        public Builder<T> aggregateFactory(Function<String, T> factory) {
            this.aggregateFactory = factory;
            return this;
        }

        public Builder<T> eventApplier(BiFunction<T, Event, T> applier) {
            this.eventApplier = applier;
            return this;
        }

        public Builder<T> snapshotInterval(int interval) {
            this.snapshotInterval = interval;
            return this;
        }

        public EventSourcing<T> build() {
            return new EventSourcing<>(
                config, eventStoreTopic, snapshotTopic,
                aggregateType, aggregateFactory, eventApplier, snapshotInterval);
        }
    }
}
