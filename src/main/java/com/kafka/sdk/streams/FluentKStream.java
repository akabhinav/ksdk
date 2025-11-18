package com.kafka.sdk.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Fluent wrapper around KStream for easier stream processing.
 */
public class FluentKStream<K, V> {

    private final KStream<K, V> stream;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public FluentKStream(KStream<K, V> stream, Serde<K> keySerde, Serde<V> valueSerde) {
        this.stream = stream;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Map values to a new type.
     */
    public <VR> FluentKStream<K, VR> mapValues(Function<V, VR> mapper, Serde<VR> newValueSerde) {
        KStream<K, VR> mapped = stream.mapValues(mapper::apply);
        return new FluentKStream<>(mapped, keySerde, newValueSerde);
    }

    /**
     * Map values to a new type (String output).
     */
    public FluentKStream<K, String> mapValuesToString(Function<V, String> mapper) {
        return mapValues(mapper, Serdes.String());
    }

    /**
     * Map both key and value.
     */
    public <KR, VR> FluentKStream<KR, VR> map(
            KeyValueMapper<K, V, KeyValue<KR, VR>> mapper,
            Serde<KR> newKeySerde,
            Serde<VR> newValueSerde) {
        KStream<KR, VR> mapped = stream.map(mapper);
        return new FluentKStream<>(mapped, newKeySerde, newValueSerde);
    }

    /**
     * Filter messages based on a predicate.
     */
    public FluentKStream<K, V> filter(Predicate<V> predicate) {
        KStream<K, V> filtered = stream.filter((k, v) -> predicate.test(v));
        return new FluentKStream<>(filtered, keySerde, valueSerde);
    }

    /**
     * Filter messages based on key and value.
     */
    public FluentKStream<K, V> filter(java.util.function.BiPredicate<K, V> predicate) {
        KStream<K, V> filtered = stream.filter(predicate::test);
        return new FluentKStream<>(filtered, keySerde, valueSerde);
    }

    /**
     * Filter out null values.
     */
    public FluentKStream<K, V> filterNotNull() {
        return filter(v -> v != null);
    }

    /**
     * FlatMap values to multiple output records.
     */
    public <VR> FluentKStream<K, VR> flatMapValues(
            Function<V, Iterable<VR>> mapper,
            Serde<VR> newValueSerde) {
        KStream<K, VR> flatMapped = stream.flatMapValues(mapper::apply);
        return new FluentKStream<>(flatMapped, keySerde, newValueSerde);
    }

    /**
     * Branch the stream based on predicates.
     */
    @SafeVarargs
    public final FluentKStream<K, V>[] branch(Predicate<V>... predicates) {
        @SuppressWarnings("unchecked")
        org.apache.kafka.streams.kstream.Predicate<K, V>[] kafkaPredicates =
            new org.apache.kafka.streams.kstream.Predicate[predicates.length];

        for (int i = 0; i < predicates.length; i++) {
            final Predicate<V> predicate = predicates[i];
            kafkaPredicates[i] = (k, v) -> predicate.test(v);
        }

        @SuppressWarnings("unchecked")
        KStream<K, V>[] branches = stream.branch(kafkaPredicates);

        @SuppressWarnings("unchecked")
        FluentKStream<K, V>[] result = new FluentKStream[branches.length];
        for (int i = 0; i < branches.length; i++) {
            result[i] = new FluentKStream<>(branches[i], keySerde, valueSerde);
        }
        return result;
    }

    /**
     * Group by key for aggregation.
     */
    public FluentKGroupedStream<K, V> groupByKey() {
        KGroupedStream<K, V> grouped = stream.groupByKey(Grouped.with(keySerde, valueSerde));
        return new FluentKGroupedStream<>(grouped, keySerde, valueSerde);
    }

    /**
     * Group by a new key.
     */
    public <KR> FluentKGroupedStream<KR, V> groupBy(
            KeyValueMapper<K, V, KR> keyMapper,
            Serde<KR> newKeySerde) {
        KGroupedStream<KR, V> grouped = stream.groupBy(keyMapper, Grouped.with(newKeySerde, valueSerde));
        return new FluentKGroupedStream<>(grouped, newKeySerde, valueSerde);
    }

    /**
     * Join with another stream.
     */
    public <VO, VR> FluentKStream<K, VR> join(
            FluentKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Serde<VR> resultSerde) {
        KStream<K, VR> joined = stream.join(
            other.getStream(),
            joiner,
            windows,
            StreamJoined.with(keySerde, valueSerde, other.getValueSerde())
        );
        return new FluentKStream<>(joined, keySerde, resultSerde);
    }

    /**
     * Left join with another stream.
     */
    public <VO, VR> FluentKStream<K, VR> leftJoin(
            FluentKStream<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            JoinWindows windows,
            Serde<VR> resultSerde) {
        KStream<K, VR> joined = stream.leftJoin(
            other.getStream(),
            joiner,
            windows,
            StreamJoined.with(keySerde, valueSerde, other.getValueSerde())
        );
        return new FluentKStream<>(joined, keySerde, resultSerde);
    }

    /**
     * Join with a table.
     */
    public <VT, VR> FluentKStream<K, VR> join(
            KTable<K, VT> table,
            ValueJoiner<V, VT, VR> joiner,
            Serde<VR> resultSerde) {
        KStream<K, VR> joined = stream.join(table, joiner);
        return new FluentKStream<>(joined, keySerde, resultSerde);
    }

    /**
     * Write to a topic.
     */
    public void to(String topic) {
        stream.to(topic, Produced.with(keySerde, valueSerde));
    }

    /**
     * Write to topics based on record content.
     */
    public void to(TopicNameExtractor<K, V> topicExtractor) {
        stream.to(topicExtractor, Produced.with(keySerde, valueSerde));
    }

    /**
     * Peek at records without modifying.
     */
    public FluentKStream<K, V> peek(ForeachAction<K, V> action) {
        KStream<K, V> peeked = stream.peek(action);
        return new FluentKStream<>(peeked, keySerde, valueSerde);
    }

    /**
     * Process records with a terminal operation.
     */
    public void foreach(ForeachAction<K, V> action) {
        stream.foreach(action);
    }

    /**
     * Print records to stdout.
     */
    public void print() {
        stream.print(Printed.toSysOut());
    }

    /**
     * Get the underlying KStream.
     */
    public KStream<K, V> getStream() {
        return stream;
    }

    /**
     * Get the key serde.
     */
    public Serde<K> getKeySerde() {
        return keySerde;
    }

    /**
     * Get the value serde.
     */
    public Serde<V> getValueSerde() {
        return valueSerde;
    }

    /**
     * Fluent wrapper for grouped streams.
     */
    public static class FluentKGroupedStream<K, V> {
        private final KGroupedStream<K, V> grouped;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        public FluentKGroupedStream(KGroupedStream<K, V> grouped, Serde<K> keySerde, Serde<V> valueSerde) {
            this.grouped = grouped;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        /**
         * Count records.
         */
        public KTable<K, Long> count() {
            return grouped.count();
        }

        /**
         * Reduce records.
         */
        public KTable<K, V> reduce(Reducer<V> reducer) {
            return grouped.reduce(reducer);
        }

        /**
         * Aggregate records.
         */
        public <VR> KTable<K, VR> aggregate(
                Initializer<VR> initializer,
                Aggregator<K, V, VR> aggregator,
                Serde<VR> resultSerde) {
            return grouped.aggregate(initializer, aggregator, Materialized.with(keySerde, resultSerde));
        }

        /**
         * Window by time for tumbling windows.
         */
        public FluentTimeWindowedKStream<K, V> windowedBy(Duration windowSize) {
            TimeWindowedKStream<K, V> windowed = grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize));
            return new FluentTimeWindowedKStream<>(windowed, keySerde, valueSerde);
        }

        /**
         * Window by time for sliding windows.
         */
        public FluentTimeWindowedKStream<K, V> slidingWindowedBy(Duration windowSize, Duration advanceBy) {
            TimeWindowedKStream<K, V> windowed = grouped.windowedBy(
                TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceBy));
            return new FluentTimeWindowedKStream<>(windowed, keySerde, valueSerde);
        }

        /**
         * Window by session.
         */
        public FluentSessionWindowedKStream<K, V> sessionWindowedBy(Duration inactivityGap) {
            SessionWindowedKStream<K, V> windowed = grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityGap));
            return new FluentSessionWindowedKStream<>(windowed, keySerde, valueSerde);
        }
    }

    /**
     * Fluent wrapper for time-windowed streams.
     */
    public static class FluentTimeWindowedKStream<K, V> {
        private final TimeWindowedKStream<K, V> windowed;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        public FluentTimeWindowedKStream(TimeWindowedKStream<K, V> windowed, Serde<K> keySerde, Serde<V> valueSerde) {
            this.windowed = windowed;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public KTable<Windowed<K>, Long> count() {
            return windowed.count();
        }

        public KTable<Windowed<K>, V> reduce(Reducer<V> reducer) {
            return windowed.reduce(reducer);
        }

        public <VR> KTable<Windowed<K>, VR> aggregate(
                Initializer<VR> initializer,
                Aggregator<K, V, VR> aggregator,
                Serde<VR> resultSerde) {
            return windowed.aggregate(initializer, aggregator, Materialized.with(keySerde, resultSerde));
        }
    }

    /**
     * Fluent wrapper for session-windowed streams.
     */
    public static class FluentSessionWindowedKStream<K, V> {
        private final SessionWindowedKStream<K, V> windowed;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        public FluentSessionWindowedKStream(SessionWindowedKStream<K, V> windowed, Serde<K> keySerde, Serde<V> valueSerde) {
            this.windowed = windowed;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public KTable<Windowed<K>, Long> count() {
            return windowed.count();
        }

        public KTable<Windowed<K>, V> reduce(Reducer<V> reducer) {
            return windowed.reduce(reducer);
        }
    }
}
