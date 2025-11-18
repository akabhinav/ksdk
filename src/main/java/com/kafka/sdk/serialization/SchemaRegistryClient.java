package com.kafka.sdk.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.sdk.core.KafkaSdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Advanced Schema Registry client with caching, versioning, and compatibility checking.
 */
public class SchemaRegistryClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryClient.class);

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Map<String, CachedSchema> schemaCache;
    private final Map<Integer, String> schemaByIdCache;
    private final Duration cacheTtl;

    public SchemaRegistryClient(String baseUrl) {
        this(baseUrl, Duration.ofMinutes(5));
    }

    public SchemaRegistryClient(String baseUrl, Duration cacheTtl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
        this.objectMapper = new ObjectMapper();
        this.schemaCache = new ConcurrentHashMap<>();
        this.schemaByIdCache = new ConcurrentHashMap<>();
        this.cacheTtl = cacheTtl;
    }

    /**
     * Register a new schema for a subject.
     */
    public int registerSchema(String subject, String schema) {
        try {
            String url = baseUrl + "/subjects/" + subject + "/versions";
            String body = objectMapper.writeValueAsString(Map.of("schema", schema));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new KafkaSdkException("Failed to register schema: " + response.body());
            }

            JsonNode json = objectMapper.readTree(response.body());
            int schemaId = json.get("id").asInt();

            // Cache the schema
            schemaByIdCache.put(schemaId, schema);
            schemaCache.put(subject, new CachedSchema(schemaId, schema, System.currentTimeMillis()));

            logger.info("Registered schema for subject {} with ID {}", subject, schemaId);
            return schemaId;
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to register schema", e);
        }
    }

    /**
     * Get schema by ID with caching.
     */
    public String getSchemaById(int schemaId) {
        String cached = schemaByIdCache.get(schemaId);
        if (cached != null) {
            return cached;
        }

        try {
            String url = baseUrl + "/schemas/ids/" + schemaId;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new KafkaSdkException("Schema not found: " + schemaId);
            }

            JsonNode json = objectMapper.readTree(response.body());
            String schema = json.get("schema").asText();

            schemaByIdCache.put(schemaId, schema);
            return schema;
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to get schema by ID", e);
        }
    }

    /**
     * Get latest schema for a subject with caching.
     */
    public SchemaMetadata getLatestSchema(String subject) {
        CachedSchema cached = schemaCache.get(subject);
        if (cached != null && !cached.isExpired(cacheTtl)) {
            return new SchemaMetadata(cached.schemaId, cached.schema, -1);
        }

        try {
            String url = baseUrl + "/subjects/" + subject + "/versions/latest";
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new KafkaSdkException("Subject not found: " + subject);
            }

            JsonNode json = objectMapper.readTree(response.body());
            int schemaId = json.get("id").asInt();
            String schema = json.get("schema").asText();
            int version = json.get("version").asInt();

            schemaCache.put(subject, new CachedSchema(schemaId, schema, System.currentTimeMillis()));
            schemaByIdCache.put(schemaId, schema);

            return new SchemaMetadata(schemaId, schema, version);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to get latest schema", e);
        }
    }

    /**
     * Check schema compatibility.
     */
    public CompatibilityResult checkCompatibility(String subject, String schema) {
        try {
            String url = baseUrl + "/compatibility/subjects/" + subject + "/versions/latest";
            String body = objectMapper.writeValueAsString(Map.of("schema", schema));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            JsonNode json = objectMapper.readTree(response.body());
            boolean isCompatible = json.get("is_compatible").asBoolean();

            return new CompatibilityResult(isCompatible,
                isCompatible ? null : "Schema is not compatible with latest version");
        } catch (Exception e) {
            return new CompatibilityResult(false, e.getMessage());
        }
    }

    /**
     * Get compatibility level for a subject.
     */
    public CompatibilityLevel getCompatibilityLevel(String subject) {
        try {
            String url = baseUrl + "/config/" + subject;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 404) {
                return getGlobalCompatibilityLevel();
            }

            JsonNode json = objectMapper.readTree(response.body());
            String level = json.get("compatibilityLevel").asText();
            return CompatibilityLevel.valueOf(level);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to get compatibility level", e);
        }
    }

    /**
     * Get global compatibility level.
     */
    public CompatibilityLevel getGlobalCompatibilityLevel() {
        try {
            String url = baseUrl + "/config";
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            JsonNode json = objectMapper.readTree(response.body());
            String level = json.get("compatibilityLevel").asText();
            return CompatibilityLevel.valueOf(level);
        } catch (Exception e) {
            return CompatibilityLevel.BACKWARD;
        }
    }

    /**
     * Set compatibility level for a subject.
     */
    public void setCompatibilityLevel(String subject, CompatibilityLevel level) {
        try {
            String url = baseUrl + "/config/" + subject;
            String body = objectMapper.writeValueAsString(Map.of("compatibility", level.name()));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new KafkaSdkException("Failed to set compatibility level: " + response.body());
            }

            logger.info("Set compatibility level for {} to {}", subject, level);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to set compatibility level", e);
        }
    }

    /**
     * Delete a subject.
     */
    public void deleteSubject(String subject) {
        try {
            String url = baseUrl + "/subjects/" + subject;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .DELETE()
                .build();

            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            schemaCache.remove(subject);
            logger.info("Deleted subject: {}", subject);
        } catch (Exception e) {
            throw new KafkaSdkException("Failed to delete subject", e);
        }
    }

    /**
     * Clear the schema cache.
     */
    public void clearCache() {
        schemaCache.clear();
        schemaByIdCache.clear();
    }

    @Override
    public void close() {
        clearCache();
    }

    public enum CompatibilityLevel {
        NONE,
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE
    }

    public record SchemaMetadata(int id, String schema, int version) {}

    public record CompatibilityResult(boolean compatible, String message) {}

    private record CachedSchema(int schemaId, String schema, long cachedAt) {
        boolean isExpired(Duration ttl) {
            return System.currentTimeMillis() - cachedAt > ttl.toMillis();
        }
    }
}
