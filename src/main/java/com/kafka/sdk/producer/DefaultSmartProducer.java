package com.kafka.sdk.producer;

import com.kafka.sdk.config.KafkaSdkConfig;
import com.kafka.sdk.core.KafkaSdkException;
import com.kafka.sdk.core.ProducerException;
import com.kafka.sdk.monitoring.MetricsRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of SmartProducer with enterprise features.
 */
public class DefaultSmartProducer<K, V> implements SmartProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSmartProducer.class);

    private final KafkaProducer<K, V> kafkaProducer;
    private final KafkaSdkConfig config;
    private final List<ProducerInterceptor<K, V>> interceptors;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiter rateLimiter;
    private final AtomicBoolean closed;
    private final AtomicBoolean inTransaction;
    private final ProducerMetrics metrics;

    // Metrics
    private final AtomicLong messagesSent;
    private final AtomicLong messagesSucceeded;
    private final AtomicLong messagesFailed;
    private final Timer sendLatencyTimer;

    public DefaultSmartProducer(KafkaSdkConfig config) {
        this(config, (Serializer<K>) new StringSerializer(), (Serializer<V>) new StringSerializer());
    }

    public DefaultSmartProducer(KafkaSdkConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.config = config;
        this.interceptors = new CopyOnWriteArrayList<>();
        this.closed = new AtomicBoolean(false);
        this.inTransaction = new AtomicBoolean(false);
        this.messagesSent = new AtomicLong(0);
        this.messagesSucceeded = new AtomicLong(0);
        this.messagesFailed = new AtomicLong(0);

        // Create Kafka producer
        Properties props = config.toProducerProperties();
        this.kafkaProducer = new KafkaProducer<>(props, keySerializer, valueSerializer);

        // Initialize transactions if configured
        if (config.getProducerConfig().getTransactionalId() != null) {
            kafkaProducer.initTransactions();
        }

        // Setup circuit breaker
        if (config.getResilienceConfig().isCircuitBreakerEnabled()) {
            CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getResilienceConfig().getCircuitBreakerFailureRateThreshold())
                .waitDurationInOpenState(config.getResilienceConfig().getCircuitBreakerWaitDurationInOpenState())
                .permittedNumberOfCallsInHalfOpenState(config.getResilienceConfig().getCircuitBreakerPermittedCallsInHalfOpen())
                .minimumNumberOfCalls(config.getResilienceConfig().getCircuitBreakerMinimumNumberOfCalls())
                .build();
            this.circuitBreaker = CircuitBreaker.of("producer-circuit-breaker", cbConfig);
        } else {
            this.circuitBreaker = null;
        }

        // Setup rate limiter
        if (config.getResilienceConfig().isRateLimiterEnabled()) {
            RateLimiterConfig rlConfig = RateLimiterConfig.custom()
                .limitForPeriod(config.getResilienceConfig().getRateLimiterLimitForPeriod())
                .limitRefreshPeriod(config.getResilienceConfig().getRateLimiterLimitRefreshPeriod())
                .timeoutDuration(config.getResilienceConfig().getRateLimiterTimeoutDuration())
                .build();
            this.rateLimiter = RateLimiter.of("producer-rate-limiter", rlConfig);
        } else {
            this.rateLimiter = null;
        }

        // Setup metrics
        this.sendLatencyTimer = MetricsRegistry.timer("kafka.producer.send.latency");
        this.metrics = new ProducerMetrics() {
            @Override
            public long getMessagesSent() {
                return messagesSent.get();
            }

            @Override
            public long getMessagesSucceeded() {
                return messagesSucceeded.get();
            }

            @Override
            public long getMessagesFailed() {
                return messagesFailed.get();
            }

            @Override
            public double getSuccessRate() {
                long sent = messagesSent.get();
                return sent > 0 ? (double) messagesSucceeded.get() / sent : 0.0;
            }

            @Override
            public Map<String, Object> getKafkaMetrics() {
                Map<String, Object> result = new HashMap<>();
                kafkaProducer.metrics().forEach((name, metric) ->
                    result.put(name.name(), metric.metricValue()));
                return result;
            }
        };

        logger.info("SmartProducer initialized with bootstrap servers: {}", config.getBootstrapServers());
    }

    @Override
    public CompletableFuture<RecordMetadata> send(String topic, V value) {
        return send(topic, null, value);
    }

    @Override
    public CompletableFuture<RecordMetadata> send(String topic, K key, V value) {
        return send(topic, null, key, value);
    }

    @Override
    public CompletableFuture<RecordMetadata> send(String topic, Integer partition, K key, V value) {
        return send(new ProducerRecord<>(topic, partition, key, value));
    }

    @Override
    public CompletableFuture<RecordMetadata> send(String topic, K key, V value, Headers headers) {
        return send(new ProducerRecord<>(topic, null, key, value, headers));
    }

    @Override
    public CompletableFuture<RecordMetadata> send(String topic, Integer partition, Long timestamp, K key, V value) {
        return send(new ProducerRecord<>(topic, partition, timestamp, key, value));
    }

    @Override
    public CompletableFuture<RecordMetadata> send(ProducerRecord<K, V> record) {
        ensureNotClosed();

        // Check circuit breaker
        if (circuitBreaker != null && !circuitBreaker.tryAcquirePermission()) {
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new KafkaSdkException(
                "Circuit breaker is open",
                KafkaSdkException.ErrorCode.CIRCUIT_BREAKER_OPEN));
            return future;
        }

        // Check rate limiter
        if (rateLimiter != null && !rateLimiter.acquirePermission()) {
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new KafkaSdkException(
                "Rate limit exceeded",
                KafkaSdkException.ErrorCode.RATE_LIMIT_EXCEEDED));
            return future;
        }

        // Apply interceptors
        ProducerRecord<K, V> enrichedRecord = applyInterceptors(record);

        // Add standard headers
        enrichedRecord = addStandardHeaders(enrichedRecord);

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        messagesSent.incrementAndGet();

        Timer.Sample sample = Timer.start();

        kafkaProducer.send(enrichedRecord, (metadata, exception) -> {
            sample.stop(sendLatencyTimer);

            if (exception != null) {
                messagesFailed.incrementAndGet();
                if (circuitBreaker != null) {
                    circuitBreaker.onError(0, java.util.concurrent.TimeUnit.MILLISECONDS, exception);
                }
                logger.error("Failed to send message to topic {}: {}", record.topic(), exception.getMessage());
                future.completeExceptionally(new ProducerException(
                    "Failed to send message",
                    exception,
                    record.topic(),
                    record.partition(),
                    record.key()));
            } else {
                messagesSucceeded.incrementAndGet();
                if (circuitBreaker != null) {
                    circuitBreaker.onSuccess(0, java.util.concurrent.TimeUnit.MILLISECONDS);
                }
                logger.debug("Message sent to topic {} partition {} offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
                future.complete(metadata);
            }
        });

        return future;
    }

    @Override
    public List<CompletableFuture<RecordMetadata>> sendBatch(String topic, List<V> values) {
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>(values.size());
        for (V value : values) {
            futures.add(send(topic, value));
        }
        return futures;
    }

    @Override
    public List<CompletableFuture<RecordMetadata>> sendBatch(String topic, Map<K, V> keyValues) {
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>(keyValues.size());
        for (Map.Entry<K, V> entry : keyValues.entrySet()) {
            futures.add(send(topic, entry.getKey(), entry.getValue()));
        }
        return futures;
    }

    @Override
    public CompletableFuture<RecordMetadata> sendPriority(String topic, K key, V value, MessagePriority priority) {
        Headers headers = new RecordHeaders();
        headers.add("x-priority", String.valueOf(priority.getLevel()).getBytes());

        // For critical messages, flush immediately after send
        CompletableFuture<RecordMetadata> future = send(topic, key, value, headers);
        if (priority == MessagePriority.CRITICAL || priority == MessagePriority.HIGH) {
            flush();
        }
        return future;
    }

    @Override
    public void beginTransaction() {
        ensureNotClosed();
        if (config.getProducerConfig().getTransactionalId() == null) {
            throw new ProducerException("Transactional ID not configured");
        }
        kafkaProducer.beginTransaction();
        inTransaction.set(true);
        logger.debug("Transaction started");
    }

    @Override
    public void commitTransaction() {
        ensureNotClosed();
        if (!inTransaction.get()) {
            throw new ProducerException("No active transaction");
        }
        kafkaProducer.commitTransaction();
        inTransaction.set(false);
        logger.debug("Transaction committed");
    }

    @Override
    public void abortTransaction() {
        ensureNotClosed();
        if (!inTransaction.get()) {
            throw new ProducerException("No active transaction");
        }
        kafkaProducer.abortTransaction();
        inTransaction.set(false);
        logger.debug("Transaction aborted");
    }

    @Override
    public CompletableFuture<RecordMetadata> sendInTransaction(String topic, K key, V value) {
        if (!inTransaction.get()) {
            throw new ProducerException("No active transaction");
        }
        return send(topic, key, value);
    }

    @Override
    public void flush() {
        ensureNotClosed();
        kafkaProducer.flush();
    }

    @Override
    public ProducerMetrics getMetrics() {
        return metrics;
    }

    @Override
    public boolean isHealthy() {
        if (closed.get()) {
            return false;
        }
        if (circuitBreaker != null && circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            return false;
        }
        return true;
    }

    @Override
    public void addInterceptor(ProducerInterceptor<K, V> interceptor) {
        interceptors.add(interceptor);
    }

    @Override
    public void removeInterceptor(ProducerInterceptor<K, V> interceptor) {
        interceptors.remove(interceptor);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (inTransaction.get()) {
                try {
                    abortTransaction();
                } catch (Exception e) {
                    logger.warn("Failed to abort transaction during close", e);
                }
            }
            kafkaProducer.close(Duration.ofSeconds(30));
            logger.info("SmartProducer closed");
        }
    }

    private void ensureNotClosed() {
        if (closed.get()) {
            throw new ProducerException("Producer is closed", KafkaSdkException.ErrorCode.PRODUCER_CLOSED, null);
        }
    }

    private ProducerRecord<K, V> applyInterceptors(ProducerRecord<K, V> record) {
        ProducerRecord<K, V> result = record;
        for (ProducerInterceptor<K, V> interceptor : interceptors) {
            result = interceptor.onSend(result);
        }
        return result;
    }

    private ProducerRecord<K, V> addStandardHeaders(ProducerRecord<K, V> record) {
        Headers headers = record.headers() != null ? record.headers() : new RecordHeaders();

        // Add correlation ID if not present
        if (headers.lastHeader("x-correlation-id") == null) {
            headers.add("x-correlation-id", UUID.randomUUID().toString().getBytes());
        }

        // Add timestamp
        headers.add("x-timestamp", String.valueOf(System.currentTimeMillis()).getBytes());

        // Add producer ID
        headers.add("x-producer-id", "kafka-sdk".getBytes());

        return new ProducerRecord<>(
            record.topic(),
            record.partition(),
            record.timestamp(),
            record.key(),
            record.value(),
            headers
        );
    }
}
