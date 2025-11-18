package com.kafka.sdk.streams;

import com.kafka.sdk.config.KafkaSdkConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Fluent builder for Kafka Streams applications.
 */
public class StreamBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

    private final KafkaSdkConfig config;
    private final StreamsBuilder streamsBuilder;
    private KafkaStreams streams;

    public StreamBuilder(KafkaSdkConfig config) {
        this.config = config;
        this.streamsBuilder = new StreamsBuilder();
    }

    /**
     * Create a stream from a topic.
     */
    public <K, V> FluentKStream<K, V> stream(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        KStream<K, V> stream = streamsBuilder.stream(topic, Consumed.with(keySerde, valueSerde));
        return new FluentKStream<>(stream, keySerde, valueSerde);
    }

    /**
     * Create a stream from a topic with String keys and values.
     */
    public FluentKStream<String, String> stream(String topic) {
        return stream(topic, Serdes.String(), Serdes.String());
    }

    /**
     * Create a table from a topic.
     */
    public <K, V> KTable<K, V> table(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return streamsBuilder.table(topic, Consumed.with(keySerde, valueSerde));
    }

    /**
     * Create a global table from a topic.
     */
    public <K, V> GlobalKTable<K, V> globalTable(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return streamsBuilder.globalTable(topic, Consumed.with(keySerde, valueSerde));
    }

    /**
     * Add a state store.
     */
    public <K, V> StreamBuilder addStateStore(String storeName, Serde<K> keySerde, Serde<V> valueSerde) {
        StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            keySerde,
            valueSerde
        );
        streamsBuilder.addStateStore(storeBuilder);
        return this;
    }

    /**
     * Build and start the streams application.
     */
    public KafkaStreams build() {
        Topology topology = streamsBuilder.build();
        Properties props = config.toStreamsProperties();
        streams = new KafkaStreams(topology, props);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (streams != null) {
                streams.close(Duration.ofSeconds(30));
            }
        }));

        return streams;
    }

    /**
     * Start the streams application.
     */
    public void start() {
        if (streams == null) {
            build();
        }
        streams.start();
        logger.info("Kafka Streams application started with application ID: {}",
            config.getStreamsConfig().getApplicationId());
    }

    /**
     * Stop the streams application.
     */
    public void stop() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            logger.info("Kafka Streams application stopped");
        }
    }

    /**
     * Get the underlying StreamsBuilder.
     */
    public StreamsBuilder getStreamsBuilder() {
        return streamsBuilder;
    }

    /**
     * Get the KafkaStreams instance.
     */
    public KafkaStreams getStreams() {
        return streams;
    }
}
