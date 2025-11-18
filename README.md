# Enterprise Kafka SDK

A comprehensive, production-ready Kafka SDK that abstracts complexity while providing enterprise-grade features for message streaming, processing, and integration.

## Features

- **Producer Capabilities**: Smart factories, intelligent partitioning, guaranteed delivery, batch optimization
- **Consumer Capabilities**: Parallel processing, offset management, rebalance handling, error recovery
- **Stream Processing**: Fluent API, stateful processing, windowing, aggregations
- **Administration**: Cluster monitoring, topic management, schema registry integration
- **Resilience**: Dead letter queues, message replay, circuit breakers, rate limiting
- **Security**: SSL/TLS, SASL authentication, data masking, audit trails
- **Developer Experience**: Annotation-based configuration, testing utilities, code generation

## Requirements

- Java 21+
- Apache Kafka 2.8+
- Maven 3.8+

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.kafka</groupId>
    <artifactId>kafka-sdk</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Basic Producer Example

```java
// Create a producer using the factory
KafkaSdkConfig config = KafkaSdkConfig.builder()
    .bootstrapServers("localhost:9092")
    .build();

try (SmartProducer<String, MyEvent> producer = ProducerFactory.create(config)) {
    producer.send("my-topic", new MyEvent("Hello, Kafka!"));
}
```

### Basic Consumer Example

```java
KafkaSdkConfig config = KafkaSdkConfig.builder()
    .bootstrapServers("localhost:9092")
    .groupId("my-consumer-group")
    .build();

SmartConsumer<String, MyEvent> consumer = ConsumerFactory.create(config);
consumer.subscribe("my-topic", event -> {
    System.out.println("Received: " + event);
});
consumer.start();
```

### Spring Boot Integration

```java
@SpringBootApplication
@EnableKafkaSdk
public class MyApplication {

    @KafkaConsumer(topics = "my-topic", groupId = "my-group")
    public void handleEvent(MyEvent event) {
        // Process event
    }
}
```

## Configuration

### YAML Configuration

```yaml
kafka-sdk:
  bootstrap-servers: localhost:9092
  producer:
    acks: all
    retries: 3
    batch-size: 16384
    compression-type: snappy
  consumer:
    group-id: my-group
    auto-offset-reset: earliest
    enable-auto-commit: false
```

### Programmatic Configuration

```java
KafkaSdkConfig config = KafkaSdkConfig.builder()
    .bootstrapServers("localhost:9092")
    .producer(ProducerConfig.builder()
        .acks(Acks.ALL)
        .retries(3)
        .compressionType(CompressionType.SNAPPY)
        .build())
    .consumer(ConsumerConfig.builder()
        .groupId("my-group")
        .autoOffsetReset(AutoOffsetReset.EARLIEST)
        .build())
    .build();
```

## Building

```bash
mvn clean install
```

## Testing

```bash
mvn test
```

## Documentation

- [API Reference](docs/api-reference.md)
- [Configuration Guide](docs/configuration.md)
- [Best Practices](docs/best-practices.md)

## License

Apache License 2.0
