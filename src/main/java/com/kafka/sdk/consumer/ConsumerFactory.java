package com.kafka.sdk.consumer;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.serialization.SerializerFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating smart consumer instances.
 */
public class ConsumerFactory {

    private static final Map<String, SmartConsumer<?, ?>> consumerPool = new ConcurrentHashMap<>();

    /**
     * Create a new consumer with string keys and values.
     */
    public static SmartConsumer<String, String> createStringConsumer(KafkaSdkConfig config) {
        return new DefaultSmartConsumer<>(config);
    }

    /**
     * Create a new consumer with custom deserializers.
     */
    public static <K, V> SmartConsumer<K, V> create(
            KafkaSdkConfig config,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return new DefaultSmartConsumer<>(config, keyDeserializer, valueDeserializer);
    }

    /**
     * Create a new consumer for JSON deserialization.
     */
    public static <V> SmartConsumer<String, V> createJsonConsumer(KafkaSdkConfig config, Class<V> valueClass) {
        Deserializer<V> valueDeserializer = SerializerFactory.jsonDeserializer(valueClass);
        return new DefaultSmartConsumer<>(config,
            new org.apache.kafka.common.serialization.StringDeserializer(),
            valueDeserializer);
    }

    /**
     * Create a new consumer for Avro deserialization.
     */
    public static <V> SmartConsumer<String, V> createAvroConsumer(
            KafkaSdkConfig config,
            String schemaRegistryUrl,
            Class<V> valueClass) {
        Deserializer<V> valueDeserializer = SerializerFactory.avroDeserializer(schemaRegistryUrl);
        return new DefaultSmartConsumer<>(config,
            new org.apache.kafka.common.serialization.StringDeserializer(),
            valueDeserializer);
    }

    /**
     * Get or create a pooled consumer instance.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> SmartConsumer<K, V> getOrCreate(
            String poolName,
            KafkaSdkConfig config,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return (SmartConsumer<K, V>) consumerPool.computeIfAbsent(poolName,
            k -> new DefaultSmartConsumer<>(config, keyDeserializer, valueDeserializer));
    }

    /**
     * Close and remove a pooled consumer.
     */
    public static void closePooledConsumer(String poolName) {
        SmartConsumer<?, ?> consumer = consumerPool.remove(poolName);
        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * Close all pooled consumers.
     */
    public static void closeAllPooledConsumers() {
        consumerPool.values().forEach(SmartConsumer::close);
        consumerPool.clear();
    }

    /**
     * Get the number of active pooled consumers.
     */
    public static int getPoolSize() {
        return consumerPool.size();
    }
}
