package com.kafka.sdk.resilience;

import com.kafka.sdk.config.KafkaSdkConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Message replay capability for reprocessing historical messages.
 */
public class MessageReplay<K, V> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MessageReplay.class);

    private final KafkaConsumer<K, V> consumer;
    private final int rateLimit; // messages per second, 0 for unlimited

    public MessageReplay(KafkaSdkConfig config) {
        this(config, (Deserializer<K>) new StringDeserializer(), (Deserializer<V>) new StringDeserializer(), 0);
    }

    public MessageReplay(KafkaSdkConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, int rateLimit) {
        Properties props = config.toConsumerProperties();
        props.put("enable.auto.commit", "false");
        props.put("group.id", "replay-" + UUID.randomUUID());
        this.consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
        this.rateLimit = rateLimit;
    }

    /**
     * Replay messages from a specific offset.
     */
    public void replayFromOffset(
            String topic,
            int partition,
            long offset,
            Consumer<ConsumerRecord<K, V>> handler) {

        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(tp));
        consumer.seek(tp, offset);

        replay(handler, record -> true);
    }

    /**
     * Replay messages from a timestamp.
     */
    public void replayFromTimestamp(
            String topic,
            long timestamp,
            Consumer<ConsumerRecord<K, V>> handler) {

        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .toList();

        consumer.assign(partitions);

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        partitions.forEach(tp -> timestampsToSearch.put(tp, timestamp));

        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);
        offsets.forEach((tp, offsetAndTimestamp) -> {
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        });

        replay(handler, record -> true);
    }

    /**
     * Replay messages within a time range.
     */
    public void replayTimeRange(
            String topic,
            Instant from,
            Instant to,
            Consumer<ConsumerRecord<K, V>> handler) {

        replayFromTimestamp(topic, from.toEpochMilli(), handler);

        // Add filter to stop at 'to' timestamp
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .toList();

        consumer.assign(partitions);

        long fromTimestamp = from.toEpochMilli();
        long toTimestamp = to.toEpochMilli();

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        partitions.forEach(tp -> timestampsToSearch.put(tp, fromTimestamp));

        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);
        offsets.forEach((tp, offsetAndTimestamp) -> {
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        });

        replay(handler, record -> record.timestamp() <= toTimestamp);
    }

    /**
     * Replay messages matching a filter.
     */
    public void replayWithFilter(
            String topic,
            Predicate<ConsumerRecord<K, V>> filter,
            Consumer<ConsumerRecord<K, V>> handler) {

        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .toList();

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        replay(handler, filter);
    }

    /**
     * Replay from beginning to end.
     */
    public void replayAll(String topic, Consumer<ConsumerRecord<K, V>> handler) {
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .toList();

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

        replay(handler, record -> {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            return record.offset() < endOffsets.get(tp) - 1;
        });
    }

    private void replay(Consumer<ConsumerRecord<K, V>> handler, Predicate<ConsumerRecord<K, V>> continueCondition) {
        long processedCount = 0;
        long lastRateLimitTime = System.currentTimeMillis();

        boolean shouldContinue = true;
        while (shouldContinue) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));

            if (records.isEmpty()) {
                // Check if we've reached the end
                boolean atEnd = true;
                for (TopicPartition tp : consumer.assignment()) {
                    long position = consumer.position(tp);
                    long endOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);
                    if (position < endOffset) {
                        atEnd = false;
                        break;
                    }
                }
                if (atEnd) {
                    break;
                }
                continue;
            }

            for (ConsumerRecord<K, V> record : records) {
                if (!continueCondition.test(record)) {
                    shouldContinue = false;
                    break;
                }

                handler.accept(record);
                processedCount++;

                // Rate limiting
                if (rateLimit > 0) {
                    long now = System.currentTimeMillis();
                    long elapsed = now - lastRateLimitTime;
                    if (elapsed < 1000 && processedCount % rateLimit == 0) {
                        try {
                            Thread.sleep(1000 - elapsed);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        lastRateLimitTime = System.currentTimeMillis();
                    }
                }
            }
        }

        logger.info("Replay completed. Processed {} messages", processedCount);
    }

    @Override
    public void close() {
        consumer.close(Duration.ofSeconds(30));
    }
}
