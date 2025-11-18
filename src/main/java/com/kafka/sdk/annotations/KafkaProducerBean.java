package com.kafka.sdk.annotations;

import java.lang.annotation.*;

/**
 * Annotation to mark a method as a Kafka producer.
 * Use with Spring Boot for automatic producer configuration.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaProducerBean {

    /**
     * Default topic to produce to.
     */
    String topic() default "";

    /**
     * Producer client ID.
     */
    String clientId() default "";

    /**
     * Acknowledgment level (0, 1, all).
     */
    String acks() default "all";

    /**
     * Enable idempotent producer.
     */
    boolean idempotent() default true;

    /**
     * Transactional ID for exactly-once semantics.
     */
    String transactionalId() default "";

    /**
     * Compression type (none, gzip, snappy, lz4, zstd).
     */
    String compression() default "snappy";

    /**
     * Batch size in bytes.
     */
    int batchSize() default 16384;

    /**
     * Linger time in milliseconds.
     */
    int lingerMs() default 1;
}
