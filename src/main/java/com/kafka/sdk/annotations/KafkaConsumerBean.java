package com.kafka.sdk.annotations;

import java.lang.annotation.*;

/**
 * Annotation to mark a method as a Kafka consumer handler.
 * Use with Spring Boot for automatic consumer configuration.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaConsumerBean {

    /**
     * Topics to consume from.
     */
    String[] topics();

    /**
     * Consumer group ID.
     */
    String groupId();

    /**
     * Client ID.
     */
    String clientId() default "";

    /**
     * Auto offset reset (earliest, latest, none).
     */
    String autoOffsetReset() default "latest";

    /**
     * Enable auto commit.
     */
    boolean autoCommit() default false;

    /**
     * Concurrency level (number of consumer threads).
     */
    int concurrency() default 1;

    /**
     * Maximum poll records.
     */
    int maxPollRecords() default 500;

    /**
     * Enable batch processing.
     */
    boolean batch() default false;

    /**
     * Error handler bean name.
     */
    String errorHandler() default "";
}
