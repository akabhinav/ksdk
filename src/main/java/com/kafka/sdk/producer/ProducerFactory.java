package com.kafka.sdk.producer;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.serialization.SerializerFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating smart producer instances.
 * Manages producer pools and handles automatic resource cleanup.
 */
public class ProducerFactory {

    private static final Map<String, SmartProducer<?, ?>> producerPool = new ConcurrentHashMap<>();

    /**
     * Create a new producer with string keys and values.
     */
    public static SmartProducer<String, String> createStringProducer(KafkaSdkConfig config) {
        return new DefaultSmartProducer<>(config);
    }

    /**
     * Create a new producer with custom serializers.
     */
    public static <K, V> SmartProducer<K, V> create(
            KafkaSdkConfig config,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        return new DefaultSmartProducer<>(config, keySerializer, valueSerializer);
    }

    /**
     * Create a new producer for JSON serialization.
     */
    public static <V> SmartProducer<String, V> createJsonProducer(KafkaSdkConfig config, Class<V> valueClass) {
        Serializer<V> valueSerializer = SerializerFactory.jsonSerializer(valueClass);
        return new DefaultSmartProducer<>(config,
            new org.apache.kafka.common.serialization.StringSerializer(),
            valueSerializer);
    }

    /**
     * Create a new producer for Avro serialization.
     */
    public static <V> SmartProducer<String, V> createAvroProducer(
            KafkaSdkConfig config,
            String schemaRegistryUrl,
            Class<V> valueClass) {
        Serializer<V> valueSerializer = SerializerFactory.avroSerializer(schemaRegistryUrl);
        return new DefaultSmartProducer<>(config,
            new org.apache.kafka.common.serialization.StringSerializer(),
            valueSerializer);
    }

    /**
     * Get or create a pooled producer instance.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> SmartProducer<K, V> getOrCreate(
            String poolName,
            KafkaSdkConfig config,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        return (SmartProducer<K, V>) producerPool.computeIfAbsent(poolName,
            k -> new DefaultSmartProducer<>(config, keySerializer, valueSerializer));
    }

    /**
     * Close and remove a pooled producer.
     */
    public static void closePooledProducer(String poolName) {
        SmartProducer<?, ?> producer = producerPool.remove(poolName);
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Close all pooled producers.
     */
    public static void closeAllPooledProducers() {
        producerPool.values().forEach(SmartProducer::close);
        producerPool.clear();
    }

    /**
     * Get the number of active pooled producers.
     */
    public static int getPoolSize() {
        return producerPool.size();
    }
}
