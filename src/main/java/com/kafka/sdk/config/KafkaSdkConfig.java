package com.kafka.sdk.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Main configuration class for the Kafka SDK.
 * Supports builder pattern and YAML/Properties file loading.
 */
public class KafkaSdkConfig {

    private final String bootstrapServers;
    private final ProducerConfig producerConfig;
    private final ConsumerConfig consumerConfig;
    private final StreamsConfig streamsConfig;
    private final AdminConfig adminConfig;
    private final SecurityConfig securityConfig;
    private final MonitoringConfig monitoringConfig;
    private final ResilienceConfig resilienceConfig;
    private final Map<String, Object> additionalProperties;

    private KafkaSdkConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.producerConfig = builder.producerConfig != null ? builder.producerConfig : ProducerConfig.builder().build();
        this.consumerConfig = builder.consumerConfig != null ? builder.consumerConfig : ConsumerConfig.builder().build();
        this.streamsConfig = builder.streamsConfig != null ? builder.streamsConfig : StreamsConfig.builder().build();
        this.adminConfig = builder.adminConfig != null ? builder.adminConfig : AdminConfig.builder().build();
        this.securityConfig = builder.securityConfig != null ? builder.securityConfig : SecurityConfig.builder().build();
        this.monitoringConfig = builder.monitoringConfig != null ? builder.monitoringConfig : MonitoringConfig.builder().build();
        this.resilienceConfig = builder.resilienceConfig != null ? builder.resilienceConfig : ResilienceConfig.builder().build();
        this.additionalProperties = new HashMap<>(builder.additionalProperties);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    public StreamsConfig getStreamsConfig() {
        return streamsConfig;
    }

    public AdminConfig getAdminConfig() {
        return adminConfig;
    }

    public SecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    public MonitoringConfig getMonitoringConfig() {
        return monitoringConfig;
    }

    public ResilienceConfig getResilienceConfig() {
        return resilienceConfig;
    }

    public Map<String, Object> getAdditionalProperties() {
        return new HashMap<>(additionalProperties);
    }

    /**
     * Converts configuration to Kafka producer properties.
     */
    public Properties toProducerProperties() {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(producerConfig.toProperties());
        props.putAll(securityConfig.toProperties());
        props.putAll(additionalProperties);
        return props;
    }

    /**
     * Converts configuration to Kafka consumer properties.
     */
    public Properties toConsumerProperties() {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(consumerConfig.toProperties());
        props.putAll(securityConfig.toProperties());
        props.putAll(additionalProperties);
        return props;
    }

    /**
     * Converts configuration to Kafka Streams properties.
     */
    public Properties toStreamsProperties() {
        Properties props = new Properties();
        props.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(streamsConfig.toProperties());
        props.putAll(securityConfig.toProperties());
        props.putAll(additionalProperties);
        return props;
    }

    /**
     * Converts configuration to Kafka admin properties.
     */
    public Properties toAdminProperties() {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(adminConfig.toProperties());
        props.putAll(securityConfig.toProperties());
        props.putAll(additionalProperties);
        return props;
    }

    public static class Builder {
        private String bootstrapServers = "localhost:9092";
        private ProducerConfig producerConfig;
        private ConsumerConfig consumerConfig;
        private StreamsConfig streamsConfig;
        private AdminConfig adminConfig;
        private SecurityConfig securityConfig;
        private MonitoringConfig monitoringConfig;
        private ResilienceConfig resilienceConfig;
        private final Map<String, Object> additionalProperties = new HashMap<>();

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder producerConfig(ProducerConfig producerConfig) {
            this.producerConfig = producerConfig;
            return this;
        }

        public Builder consumerConfig(ConsumerConfig consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }

        public Builder streamsConfig(StreamsConfig streamsConfig) {
            this.streamsConfig = streamsConfig;
            return this;
        }

        public Builder adminConfig(AdminConfig adminConfig) {
            this.adminConfig = adminConfig;
            return this;
        }

        public Builder securityConfig(SecurityConfig securityConfig) {
            this.securityConfig = securityConfig;
            return this;
        }

        public Builder monitoringConfig(MonitoringConfig monitoringConfig) {
            this.monitoringConfig = monitoringConfig;
            return this;
        }

        public Builder resilienceConfig(ResilienceConfig resilienceConfig) {
            this.resilienceConfig = resilienceConfig;
            return this;
        }

        public Builder additionalProperty(String key, Object value) {
            this.additionalProperties.put(key, value);
            return this;
        }

        public Builder additionalProperties(Map<String, Object> properties) {
            this.additionalProperties.putAll(properties);
            return this;
        }

        public KafkaSdkConfig build() {
            return new KafkaSdkConfig(this);
        }
    }
}
