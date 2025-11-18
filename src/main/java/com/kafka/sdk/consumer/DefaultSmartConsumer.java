package com.kafka.sdk.consumer;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.core.ConsumerException;
import com.kafka.sdk.core.KafkaSdkException;
import com.kafka.sdk.monitoring.MetricsRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Default implementation of SmartConsumer with enterprise features.
 */
public class DefaultSmartConsumer<K, V> implements SmartConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSmartConsumer.class);

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final KafkaSdkConfig config;
    private final ExecutorService executorService;
    private final List<RebalanceListener> rebalanceListeners;
    private final List<MessageFilter<K, V>> filters;
    private final AtomicBoolean running;
    private final AtomicBoolean paused;
    private final AtomicBoolean closed;
    private final String groupId;

    private Consumer<ConsumerRecord<K, V>> messageHandler;
    private ErrorHandler<K, V> errorHandler;
    private Future<?> consumeTask;

    // Metrics
    private final AtomicLong messagesConsumed;
    private final AtomicLong messagesProcessed;
    private final AtomicLong messagesFailed;
    private final ConsumerMetrics metrics;

    public DefaultSmartConsumer(KafkaSdkConfig config) {
        this(config, (Deserializer<K>) new StringDeserializer(), (Deserializer<V>) new StringDeserializer());
    }

    public DefaultSmartConsumer(KafkaSdkConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.config = config;
        this.rebalanceListeners = new CopyOnWriteArrayList<>();
        this.filters = new CopyOnWriteArrayList<>();
        this.running = new AtomicBoolean(false);
        this.paused = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
        this.messagesConsumed = new AtomicLong(0);
        this.messagesProcessed = new AtomicLong(0);
        this.messagesFailed = new AtomicLong(0);

        // Create Kafka consumer
        Properties props = config.toConsumerProperties();
        this.kafkaConsumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
        this.groupId = config.getConsumerConfig().getGroupId();

        // Create thread pool for parallel processing
        int numThreads = Runtime.getRuntime().availableProcessors();
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();

        // Default error handler
        this.errorHandler = (record, exception) -> {
            logger.error("Error processing record from topic {} partition {} offset {}: {}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());
            return ErrorAction.SKIP;
        };

        // Setup metrics
        this.metrics = new ConsumerMetrics() {
            @Override
            public long getMessagesConsumed() {
                return messagesConsumed.get();
            }

            @Override
            public long getMessagesProcessed() {
                return messagesProcessed.get();
            }

            @Override
            public long getMessagesFailed() {
                return messagesFailed.get();
            }

            @Override
            public double getProcessingRate() {
                long consumed = messagesConsumed.get();
                return consumed > 0 ? (double) messagesProcessed.get() / consumed : 0.0;
            }

            @Override
            public Map<String, Object> getKafkaMetrics() {
                Map<String, Object> result = new HashMap<>();
                kafkaConsumer.metrics().forEach((name, metric) ->
                    result.put(name.name(), metric.metricValue()));
                return result;
            }
        };

        logger.info("SmartConsumer initialized for group: {}", groupId);
    }

    @Override
    public void subscribe(Collection<String> topics, Consumer<ConsumerRecord<K, V>> handler) {
        ensureNotClosed();
        this.messageHandler = handler;
        kafkaConsumer.subscribe(topics, new InternalRebalanceListener());
        logger.info("Subscribed to topics: {}", topics);
    }

    @Override
    public void subscribe(String topic, Consumer<ConsumerRecord<K, V>> handler) {
        subscribe(Collections.singletonList(topic), handler);
    }

    @Override
    public void subscribePattern(String pattern, Consumer<ConsumerRecord<K, V>> handler) {
        ensureNotClosed();
        this.messageHandler = handler;
        kafkaConsumer.subscribe(Pattern.compile(pattern), new InternalRebalanceListener());
        logger.info("Subscribed to pattern: {}", pattern);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions, Consumer<ConsumerRecord<K, V>> handler) {
        ensureNotClosed();
        this.messageHandler = handler;
        kafkaConsumer.assign(partitions);
        logger.info("Assigned partitions: {}", partitions);
    }

    @Override
    public void start() {
        ensureNotClosed();
        if (messageHandler == null) {
            throw new ConsumerException("No message handler set. Call subscribe() first.");
        }
        if (running.compareAndSet(false, true)) {
            consumeTask = CompletableFuture.runAsync(this::consumeLoop, executorService);
            logger.info("Consumer started");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            kafkaConsumer.wakeup();
            logger.info("Consumer stopped");
        }
    }

    @Override
    public void pause() {
        if (!paused.get()) {
            kafkaConsumer.pause(kafkaConsumer.assignment());
            paused.set(true);
            logger.info("Consumer paused");
        }
    }

    @Override
    public void resume() {
        if (paused.get()) {
            kafkaConsumer.resume(kafkaConsumer.assignment());
            paused.set(false);
            logger.info("Consumer resumed");
        }
    }

    @Override
    public void commitSync() {
        kafkaConsumer.commitSync();
    }

    @Override
    public void commitAsync() {
        kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                logger.error("Async commit failed", exception);
            }
        });
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitSync(offsets);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        kafkaConsumer.seek(partition, offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToEnd(partitions);
    }

    @Override
    public void seekToTimestamp(Collection<TopicPartition> partitions, long timestamp) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        partitions.forEach(tp -> timestampsToSearch.put(tp, timestamp));
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(timestampsToSearch);
        offsets.forEach((tp, offsetAndTimestamp) -> {
            if (offsetAndTimestamp != null) {
                kafkaConsumer.seek(tp, offsetAndTimestamp.offset());
            }
        });
    }

    @Override
    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    @Override
    public Map<TopicPartition, Long> getLag() {
        Map<TopicPartition, Long> lag = new HashMap<>();
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignment);

        for (TopicPartition tp : assignment) {
            long currentOffset = kafkaConsumer.position(tp);
            long endOffset = endOffsets.getOrDefault(tp, 0L);
            lag.put(tp, endOffset - currentOffset);
        }
        return lag;
    }

    @Override
    public ConsumerMetrics getMetrics() {
        return metrics;
    }

    @Override
    public boolean isHealthy() {
        return !closed.get() && running.get();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void addRebalanceListener(RebalanceListener listener) {
        rebalanceListeners.add(listener);
    }

    @Override
    public void removeRebalanceListener(RebalanceListener listener) {
        rebalanceListeners.remove(listener);
    }

    @Override
    public void setErrorHandler(ErrorHandler<K, V> errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public void addFilter(MessageFilter<K, V> filter) {
        filters.add(filter);
    }

    @Override
    public void removeFilter(MessageFilter<K, V> filter) {
        filters.remove(filter);
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            stop();
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            kafkaConsumer.close(Duration.ofSeconds(30));
            logger.info("SmartConsumer closed");
        }
    }

    private void consumeLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<K, V> record : records) {
                    messagesConsumed.incrementAndGet();
                    MetricsRegistry.increment("kafka.consumer.messages.consumed",
                        "topic", record.topic());

                    // Apply filters
                    if (!passesFilters(record)) {
                        continue;
                    }

                    try {
                        processRecord(record);
                        messagesProcessed.incrementAndGet();
                    } catch (Exception e) {
                        messagesFailed.incrementAndGet();
                        handleProcessingError(record, e);
                    }
                }

                // Commit offsets if auto-commit is disabled
                if (!config.getConsumerConfig().isEnableAutoCommit() && !records.isEmpty()) {
                    commitAsync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } catch (Exception e) {
            logger.error("Error in consume loop", e);
            throw e;
        }
    }

    private boolean passesFilters(ConsumerRecord<K, V> record) {
        for (MessageFilter<K, V> filter : filters) {
            if (!filter.accept(record)) {
                return false;
            }
        }
        return true;
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        messageHandler.accept(record);
    }

    private void handleProcessingError(ConsumerRecord<K, V> record, Exception exception) {
        ErrorAction action = errorHandler.handleError(record, exception);

        switch (action) {
            case RETRY:
                // Seek back to retry
                kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                break;
            case SKIP:
                // Do nothing, continue with next record
                break;
            case DEAD_LETTER_QUEUE:
                // TODO: Publish to DLQ
                logger.warn("DLQ publishing not yet implemented for record: {}", record);
                break;
            case STOP:
                stop();
                break;
        }
    }

    private void ensureNotClosed() {
        if (closed.get()) {
            throw new ConsumerException("Consumer is closed", KafkaSdkException.ErrorCode.CONSUMER_CLOSED, groupId);
        }
    }

    private class InternalRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("Partitions revoked: {}", partitions);
            for (RebalanceListener listener : rebalanceListeners) {
                listener.onPartitionsRevoked(partitions);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("Partitions assigned: {}", partitions);
            for (RebalanceListener listener : rebalanceListeners) {
                listener.onPartitionsAssigned(partitions);
            }
        }
    }
}
