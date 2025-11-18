package com.kafka.sdk.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.sdk.core.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating serializers and deserializers.
 */
public class SerializerFactory {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    /**
     * Create a JSON serializer for the given class.
     */
    public static <T> Serializer<T> jsonSerializer(Class<T> clazz) {
        return new JsonSerializer<>();
    }

    /**
     * Create a JSON deserializer for the given class.
     */
    public static <T> Deserializer<T> jsonDeserializer(Class<T> clazz) {
        return new JsonDeserializer<>(clazz);
    }

    /**
     * Create an Avro serializer with schema registry.
     */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> avroSerializer(String schemaRegistryUrl) {
        try {
            Class<?> serializerClass = Class.forName("io.confluent.kafka.serializers.KafkaAvroSerializer");
            Serializer<T> serializer = (Serializer<T>) serializerClass.getDeclaredConstructor().newInstance();
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            serializer.configure(config, false);
            return serializer;
        } catch (Exception e) {
            throw new SerializationException("Failed to create Avro serializer", e);
        }
    }

    /**
     * Create an Avro deserializer with schema registry.
     */
    @SuppressWarnings("unchecked")
    public static <T> Deserializer<T> avroDeserializer(String schemaRegistryUrl) {
        try {
            Class<?> deserializerClass = Class.forName("io.confluent.kafka.serializers.KafkaAvroDeserializer");
            Deserializer<T> deserializer = (Deserializer<T>) deserializerClass.getDeclaredConstructor().newInstance();
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            config.put("specific.avro.reader", true);
            deserializer.configure(config, false);
            return deserializer;
        } catch (Exception e) {
            throw new SerializationException("Failed to create Avro deserializer", e);
        }
    }

    /**
     * JSON Serializer implementation.
     */
    public static class JsonSerializer<T> implements Serializer<T> {
        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Failed to serialize to JSON", e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public void close() {}
    }

    /**
     * JSON Deserializer implementation.
     */
    public static class JsonDeserializer<T> implements Deserializer<T> {
        private final Class<T> clazz;

        public JsonDeserializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                return objectMapper.readValue(data, clazz);
            } catch (Exception e) {
                throw new SerializationException("Failed to deserialize from JSON", e, clazz, data);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public void close() {}
    }

    /**
     * Get the shared ObjectMapper instance.
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
