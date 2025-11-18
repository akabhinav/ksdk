package com.kafka.sdk.monitoring;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Distributed tracing support using OpenTelemetry.
 */
public class DistributedTracing {

    private static final Logger logger = LoggerFactory.getLogger(DistributedTracing.class);
    private static final String INSTRUMENTATION_NAME = "kafka-sdk";

    private final Tracer tracer;

    public DistributedTracing() {
        this.tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
    }

    public DistributedTracing(Tracer tracer) {
        this.tracer = tracer;
    }

    /**
     * Create a producer span and inject context into headers.
     */
    public <K, V> TracingContext startProducerSpan(ProducerRecord<K, V> record) {
        SpanBuilder spanBuilder = tracer.spanBuilder("kafka.produce")
            .setSpanKind(SpanKind.PRODUCER)
            .setAttribute("messaging.system", "kafka")
            .setAttribute("messaging.destination", record.topic())
            .setAttribute("messaging.destination_kind", "topic");

        if (record.partition() != null) {
            spanBuilder.setAttribute("messaging.kafka.partition", record.partition());
        }

        Span span = spanBuilder.startSpan();

        // Inject trace context into headers
        Context context = Context.current().with(span);
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
            .inject(context, record.headers(), HeaderSetter.INSTANCE);

        return new TracingContext(span, context);
    }

    /**
     * Create a consumer span by extracting context from headers.
     */
    public <K, V> TracingContext startConsumerSpan(ConsumerRecord<K, V> record) {
        // Extract context from headers
        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
            .extract(Context.current(), record.headers(), HeaderGetter.INSTANCE);

        SpanBuilder spanBuilder = tracer.spanBuilder("kafka.consume")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .setAttribute("messaging.system", "kafka")
            .setAttribute("messaging.destination", record.topic())
            .setAttribute("messaging.destination_kind", "topic")
            .setAttribute("messaging.kafka.partition", record.partition())
            .setAttribute("messaging.kafka.offset", record.offset());

        if (record.key() != null) {
            spanBuilder.setAttribute("messaging.kafka.message_key", record.key().toString());
        }

        Span span = spanBuilder.startSpan();
        Context context = extractedContext.with(span);

        return new TracingContext(span, context);
    }

    /**
     * Create a processing span.
     */
    public TracingContext startProcessingSpan(String operationName, TracingContext parentContext) {
        SpanBuilder spanBuilder = tracer.spanBuilder(operationName)
            .setSpanKind(SpanKind.INTERNAL);

        if (parentContext != null) {
            spanBuilder.setParent(parentContext.context());
        }

        Span span = spanBuilder.startSpan();
        Context context = (parentContext != null ? parentContext.context() : Context.current()).with(span);

        return new TracingContext(span, context);
    }

    /**
     * Execute a traced operation.
     */
    public <T> T traceOperation(String operationName, Supplier<T> operation) {
        Span span = tracer.spanBuilder(operationName)
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

        try (Scope scope = span.makeCurrent()) {
            return operation.get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Execute a traced operation with no return value.
     */
    public void traceOperation(String operationName, Runnable operation) {
        Span span = tracer.spanBuilder(operationName)
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

        try (Scope scope = span.makeCurrent()) {
            operation.run();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Add an event to the current span.
     */
    public void addEvent(String eventName, Map<String, String> attributes) {
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.isRecording()) {
            var builder = currentSpan.addEvent(eventName);
            if (attributes != null) {
                attributes.forEach(builder::setAttribute);
            }
        }
    }

    /**
     * Set an attribute on the current span.
     */
    public void setAttribute(String key, String value) {
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.isRecording()) {
            currentSpan.setAttribute(key, value);
        }
    }

    /**
     * Record an exception on the current span.
     */
    public void recordException(Throwable throwable) {
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.isRecording()) {
            currentSpan.recordException(throwable);
            currentSpan.setStatus(StatusCode.ERROR, throwable.getMessage());
        }
    }

    /**
     * Get the current trace ID.
     */
    public String getCurrentTraceId() {
        Span currentSpan = Span.current();
        if (currentSpan != null) {
            return currentSpan.getSpanContext().getTraceId();
        }
        return null;
    }

    /**
     * Get the current span ID.
     */
    public String getCurrentSpanId() {
        Span currentSpan = Span.current();
        if (currentSpan != null) {
            return currentSpan.getSpanContext().getSpanId();
        }
        return null;
    }

    public record TracingContext(Span span, Context context) implements AutoCloseable {
        public Scope makeCurrent() {
            return span.makeCurrent();
        }

        public void setStatus(StatusCode statusCode, String description) {
            span.setStatus(statusCode, description);
        }

        public void recordException(Throwable throwable) {
            span.recordException(throwable);
        }

        public void setAttribute(String key, String value) {
            span.setAttribute(key, value);
        }

        public void setAttribute(String key, long value) {
            span.setAttribute(key, value);
        }

        @Override
        public void close() {
            span.end();
        }
    }

    private static class HeaderSetter implements TextMapSetter<Headers> {
        static final HeaderSetter INSTANCE = new HeaderSetter();

        @Override
        public void set(Headers carrier, String key, String value) {
            if (carrier != null) {
                carrier.remove(key);
                carrier.add(key, value.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static class HeaderGetter implements TextMapGetter<Headers> {
        static final HeaderGetter INSTANCE = new HeaderGetter();

        @Override
        public Iterable<String> keys(Headers carrier) {
            Map<String, String> map = new HashMap<>();
            for (Header header : carrier) {
                map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
            return map.keySet();
        }

        @Override
        public String get(Headers carrier, String key) {
            Header header = carrier.lastHeader(key);
            return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
        }
    }
}
