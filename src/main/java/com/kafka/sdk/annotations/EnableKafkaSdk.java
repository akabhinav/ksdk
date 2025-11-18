package com.kafka.sdk.annotations;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enable Kafka SDK auto-configuration for Spring Boot applications.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaSdkAutoConfiguration.class)
public @interface EnableKafkaSdk {

    /**
     * Enable metrics collection.
     */
    boolean metrics() default true;

    /**
     * Enable health checks.
     */
    boolean healthChecks() default true;

    /**
     * Enable audit logging.
     */
    boolean audit() default false;

    /**
     * Enable data masking for logs.
     */
    boolean dataMasking() default false;
}
