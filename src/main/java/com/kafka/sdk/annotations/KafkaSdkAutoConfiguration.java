package com.kafka.sdk.annotations;

import com.kafka.sdk.admin.KafkaAdminClient;
import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.monitoring.MetricsRegistry;
import com.kafka.sdk.producer.ProducerFactory;
import com.kafka.sdk.producer.SmartProducer;
import com.kafka.sdk.resilience.HealthCheck;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot auto-configuration for Kafka SDK.
 */
@Configuration
@ConditionalOnClass(KafkaSdkConfig.class)
public class KafkaSdkAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "kafka-sdk")
    @ConditionalOnMissingBean
    public KafkaSdkProperties kafkaSdkProperties() {
        return new KafkaSdkProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaSdkConfig kafkaSdkConfig(KafkaSdkProperties properties) {
        return KafkaSdkConfig.builder()
            .bootstrapServers(properties.getBootstrapServers())
            .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public SmartProducer<String, String> defaultProducer(KafkaSdkConfig config) {
        return ProducerFactory.createStringProducer(config);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kafka-sdk", name = "admin.enabled", havingValue = "true", matchIfMissing = true)
    public KafkaAdminClient kafkaAdminClient(KafkaSdkConfig config) {
        return new KafkaAdminClient(config);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kafka-sdk", name = "health.enabled", havingValue = "true", matchIfMissing = true)
    public HealthCheck healthCheck(KafkaSdkConfig config) {
        return new HealthCheck(config);
    }

    /**
     * Properties class for Spring Boot configuration binding.
     */
    public static class KafkaSdkProperties {

        private String bootstrapServers = "localhost:9092";
        private ProducerProperties producer = new ProducerProperties();
        private ConsumerProperties consumer = new ConsumerProperties();
        private AdminProperties admin = new AdminProperties();
        private HealthProperties health = new HealthProperties();

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public ProducerProperties getProducer() {
            return producer;
        }

        public void setProducer(ProducerProperties producer) {
            this.producer = producer;
        }

        public ConsumerProperties getConsumer() {
            return consumer;
        }

        public void setConsumer(ConsumerProperties consumer) {
            this.consumer = consumer;
        }

        public AdminProperties getAdmin() {
            return admin;
        }

        public void setAdmin(AdminProperties admin) {
            this.admin = admin;
        }

        public HealthProperties getHealth() {
            return health;
        }

        public void setHealth(HealthProperties health) {
            this.health = health;
        }

        public static class ProducerProperties {
            private String acks = "all";
            private int retries = 3;
            private int batchSize = 16384;
            private String compressionType = "snappy";

            public String getAcks() {
                return acks;
            }

            public void setAcks(String acks) {
                this.acks = acks;
            }

            public int getRetries() {
                return retries;
            }

            public void setRetries(int retries) {
                this.retries = retries;
            }

            public int getBatchSize() {
                return batchSize;
            }

            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }

            public String getCompressionType() {
                return compressionType;
            }

            public void setCompressionType(String compressionType) {
                this.compressionType = compressionType;
            }
        }

        public static class ConsumerProperties {
            private String groupId;
            private String autoOffsetReset = "latest";
            private boolean enableAutoCommit = false;

            public String getGroupId() {
                return groupId;
            }

            public void setGroupId(String groupId) {
                this.groupId = groupId;
            }

            public String getAutoOffsetReset() {
                return autoOffsetReset;
            }

            public void setAutoOffsetReset(String autoOffsetReset) {
                this.autoOffsetReset = autoOffsetReset;
            }

            public boolean isEnableAutoCommit() {
                return enableAutoCommit;
            }

            public void setEnableAutoCommit(boolean enableAutoCommit) {
                this.enableAutoCommit = enableAutoCommit;
            }
        }

        public static class AdminProperties {
            private boolean enabled = true;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }
        }

        public static class HealthProperties {
            private boolean enabled = true;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }
        }
    }
}
