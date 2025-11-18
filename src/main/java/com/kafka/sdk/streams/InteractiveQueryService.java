package com.kafka.sdk.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * REST API service for interactive queries on Kafka Streams state stores.
 */
public class InteractiveQueryService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(InteractiveQueryService.class);

    private final KafkaStreams streams;
    private final ObjectMapper objectMapper;
    private final int port;
    private HttpServer server;

    public InteractiveQueryService(KafkaStreams streams, int port) {
        this.streams = streams;
        this.port = port;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Start the REST API server.
     */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        // Key-value store endpoints
        server.createContext("/state/keyvalue", this::handleKeyValueQuery);
        server.createContext("/state/keyvalue/range", this::handleRangeQuery);
        server.createContext("/state/keyvalue/all", this::handleAllQuery);

        // Windowed store endpoints
        server.createContext("/state/windowed", this::handleWindowedQuery);

        // Metadata endpoints
        server.createContext("/metadata/stores", this::handleStoreMetadata);
        server.createContext("/metadata/key", this::handleKeyMetadata);

        // Health endpoint
        server.createContext("/health", this::handleHealth);

        server.start();
        logger.info("Interactive Query Service started on port {}", port);
    }

    /**
     * Query a key-value store by key.
     */
    private void handleKeyValueQuery(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("error", "Method not allowed"));
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String storeName = params.get("store");
        String key = params.get("key");

        if (storeName == null || key == null) {
            sendResponse(exchange, 400, Map.of("error", "Missing store or key parameter"));
            return;
        }

        try {
            ReadOnlyKeyValueStore<String, Object> store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
            );

            Object value = store.get(key);
            if (value != null) {
                sendResponse(exchange, 200, Map.of("key", key, "value", value));
            } else {
                sendResponse(exchange, 404, Map.of("error", "Key not found"));
            }
        } catch (Exception e) {
            logger.error("Error querying store", e);
            sendResponse(exchange, 500, Map.of("error", e.getMessage()));
        }
    }

    /**
     * Query a range of keys from a key-value store.
     */
    private void handleRangeQuery(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("error", "Method not allowed"));
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String storeName = params.get("store");
        String from = params.get("from");
        String to = params.get("to");

        if (storeName == null) {
            sendResponse(exchange, 400, Map.of("error", "Missing store parameter"));
            return;
        }

        try {
            ReadOnlyKeyValueStore<String, Object> store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
            );

            List<Map<String, Object>> results = new ArrayList<>();
            KeyValueIterator<String, Object> iterator;

            if (from != null && to != null) {
                iterator = store.range(from, to);
            } else {
                iterator = store.all();
            }

            try {
                while (iterator.hasNext()) {
                    KeyValue<String, Object> kv = iterator.next();
                    results.add(Map.of("key", kv.key, "value", kv.value));
                }
            } finally {
                iterator.close();
            }

            sendResponse(exchange, 200, Map.of("results", results, "count", results.size()));
        } catch (Exception e) {
            logger.error("Error querying range", e);
            sendResponse(exchange, 500, Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get all entries from a key-value store.
     */
    private void handleAllQuery(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("error", "Method not allowed"));
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String storeName = params.get("store");
        int limit = Integer.parseInt(params.getOrDefault("limit", "100"));

        if (storeName == null) {
            sendResponse(exchange, 400, Map.of("error", "Missing store parameter"));
            return;
        }

        try {
            ReadOnlyKeyValueStore<String, Object> store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
            );

            List<Map<String, Object>> results = new ArrayList<>();
            try (KeyValueIterator<String, Object> iterator = store.all()) {
                int count = 0;
                while (iterator.hasNext() && count < limit) {
                    KeyValue<String, Object> kv = iterator.next();
                    results.add(Map.of("key", kv.key, "value", kv.value));
                    count++;
                }
            }

            sendResponse(exchange, 200, Map.of(
                "results", results,
                "count", results.size(),
                "approximateNumEntries", store.approximateNumEntries()
            ));
        } catch (Exception e) {
            logger.error("Error querying all", e);
            sendResponse(exchange, 500, Map.of("error", e.getMessage()));
        }
    }

    /**
     * Query a windowed store.
     */
    private void handleWindowedQuery(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("error", "Method not allowed"));
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String storeName = params.get("store");
        String key = params.get("key");
        long from = Long.parseLong(params.getOrDefault("from", "0"));
        long to = Long.parseLong(params.getOrDefault("to", String.valueOf(System.currentTimeMillis())));

        if (storeName == null || key == null) {
            sendResponse(exchange, 400, Map.of("error", "Missing store or key parameter"));
            return;
        }

        try {
            ReadOnlyWindowStore<String, Object> store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore())
            );

            List<Map<String, Object>> results = new ArrayList<>();
            try (WindowStoreIterator<Object> iterator = store.fetch(key,
                java.time.Instant.ofEpochMilli(from), java.time.Instant.ofEpochMilli(to))) {
                while (iterator.hasNext()) {
                    KeyValue<Long, Object> kv = iterator.next();
                    results.add(Map.of(
                        "windowStart", kv.key,
                        "value", kv.value
                    ));
                }
            }

            sendResponse(exchange, 200, Map.of("key", key, "windows", results));
        } catch (Exception e) {
            logger.error("Error querying windowed store", e);
            sendResponse(exchange, 500, Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get metadata about all stores.
     */
    private void handleStoreMetadata(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("error", "Method not allowed"));
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String storeName = params.get("store");

        if (storeName != null) {
            Collection<StreamsMetadata> metadata = streams.streamsMetadataForStore(storeName);
            List<Map<String, Object>> hosts = new ArrayList<>();

            for (StreamsMetadata m : metadata) {
                hosts.add(Map.of(
                    "host", m.host(),
                    "port", m.port(),
                    "stateStoreNames", m.stateStoreNames(),
                    "topicPartitions", m.topicPartitions().toString()
                ));
            }

            sendResponse(exchange, 200, Map.of("store", storeName, "hosts", hosts));
        } else {
            Collection<StreamsMetadata> allMetadata = streams.metadataForAllStreamsClients();
            List<Map<String, Object>> hosts = new ArrayList<>();

            for (StreamsMetadata m : allMetadata) {
                hosts.add(Map.of(
                    "host", m.host(),
                    "port", m.port(),
                    "stateStoreNames", m.stateStoreNames()
                ));
            }

            sendResponse(exchange, 200, Map.of("hosts", hosts));
        }
    }

    /**
     * Get metadata for a specific key.
     */
    private void handleKeyMetadata(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("error", "Method not allowed"));
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String storeName = params.get("store");
        String key = params.get("key");

        if (storeName == null || key == null) {
            sendResponse(exchange, 400, Map.of("error", "Missing store or key parameter"));
            return;
        }

        KeyQueryMetadata metadata = streams.queryMetadataForKey(
            storeName,
            key,
            org.apache.kafka.common.serialization.Serdes.String().serializer()
        );

        if (metadata == null || metadata.equals(KeyQueryMetadata.NOT_AVAILABLE)) {
            sendResponse(exchange, 404, Map.of("error", "Metadata not available"));
            return;
        }

        sendResponse(exchange, 200, Map.of(
            "activeHost", Map.of(
                "host", metadata.activeHost().host(),
                "port", metadata.activeHost().port()
            ),
            "standbyHosts", metadata.standbyHosts().stream()
                .map(h -> Map.of("host", h.host(), "port", h.port()))
                .toList(),
            "partition", metadata.partition()
        ));
    }

    /**
     * Health check endpoint.
     */
    private void handleHealth(HttpExchange exchange) throws IOException {
        KafkaStreams.State state = streams.state();
        boolean healthy = state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING;

        Map<String, Object> response = Map.of(
            "status", healthy ? "UP" : "DOWN",
            "state", state.name()
        );

        sendResponse(exchange, healthy ? 200 : 503, response);
    }

    private void sendResponse(HttpExchange exchange, int statusCode, Object response) throws IOException {
        byte[] responseBytes = objectMapper.writeValueAsBytes(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query != null && !query.isEmpty()) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == 2) {
                    params.put(pair[0], java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
                }
            }
        }
        return params;
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
            logger.info("Interactive Query Service stopped");
        }
    }
}
