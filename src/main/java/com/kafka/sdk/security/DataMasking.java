package com.kafka.sdk.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Data masking and PII sanitization utilities.
 */
public class DataMasking {

    private static final Logger logger = LoggerFactory.getLogger(DataMasking.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Common PII patterns
    private static final Map<String, Pattern> PII_PATTERNS = new HashMap<>();
    private static final Set<String> SENSITIVE_FIELD_NAMES = new HashSet<>();

    static {
        // Common patterns for PII detection
        PII_PATTERNS.put("EMAIL", Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}"));
        PII_PATTERNS.put("PHONE", Pattern.compile("\\+?\\d{1,4}[\\s.-]?\\(?\\d{1,3}\\)?[\\s.-]?\\d{1,4}[\\s.-]?\\d{1,9}"));
        PII_PATTERNS.put("SSN", Pattern.compile("\\d{3}-\\d{2}-\\d{4}"));
        PII_PATTERNS.put("CREDIT_CARD", Pattern.compile("\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}"));
        PII_PATTERNS.put("IP_ADDRESS", Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"));

        // Common sensitive field names
        SENSITIVE_FIELD_NAMES.addAll(Arrays.asList(
            "password", "secret", "token", "apiKey", "api_key", "apikey",
            "ssn", "socialSecurityNumber", "social_security_number",
            "creditCard", "credit_card", "cardNumber", "card_number",
            "cvv", "pin", "dob", "dateOfBirth", "date_of_birth",
            "email", "phone", "phoneNumber", "phone_number",
            "address", "zipCode", "zip_code", "postalCode", "postal_code"
        ));
    }

    /**
     * Mask a string value by replacing with asterisks.
     */
    public static String mask(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        if (value.length() <= 4) {
            return "****";
        }
        return value.substring(0, 2) + "*".repeat(value.length() - 4) + value.substring(value.length() - 2);
    }

    /**
     * Mask email address.
     */
    public static String maskEmail(String email) {
        if (email == null || !email.contains("@")) {
            return "****@****.***";
        }
        String[] parts = email.split("@");
        String localPart = parts[0].length() > 2 ?
            parts[0].charAt(0) + "***" + parts[0].charAt(parts[0].length() - 1) :
            "***";
        String domainPart = parts.length > 1 && parts[1].length() > 4 ?
            parts[1].charAt(0) + "***" + parts[1].substring(parts[1].length() - 4) :
            "***";
        return localPart + "@" + domainPart;
    }

    /**
     * Mask phone number.
     */
    public static String maskPhone(String phone) {
        if (phone == null || phone.length() < 4) {
            return "****";
        }
        return "*".repeat(phone.length() - 4) + phone.substring(phone.length() - 4);
    }

    /**
     * Mask credit card number.
     */
    public static String maskCreditCard(String cardNumber) {
        if (cardNumber == null || cardNumber.length() < 4) {
            return "****";
        }
        String digitsOnly = cardNumber.replaceAll("[^0-9]", "");
        if (digitsOnly.length() < 4) {
            return "****";
        }
        return "*".repeat(digitsOnly.length() - 4) + digitsOnly.substring(digitsOnly.length() - 4);
    }

    /**
     * Detect PII in a string.
     */
    public static Map<String, List<String>> detectPII(String text) {
        Map<String, List<String>> detected = new HashMap<>();

        for (Map.Entry<String, Pattern> entry : PII_PATTERNS.entrySet()) {
            var matcher = entry.getValue().matcher(text);
            List<String> matches = new ArrayList<>();
            while (matcher.find()) {
                matches.add(matcher.group());
            }
            if (!matches.isEmpty()) {
                detected.put(entry.getKey(), matches);
            }
        }

        return detected;
    }

    /**
     * Mask all detected PII in a string.
     */
    public static String maskPII(String text) {
        String result = text;

        for (Map.Entry<String, Pattern> entry : PII_PATTERNS.entrySet()) {
            result = entry.getValue().matcher(result).replaceAll(match -> {
                String value = match.group();
                return switch (entry.getKey()) {
                    case "EMAIL" -> maskEmail(value);
                    case "PHONE" -> maskPhone(value);
                    case "CREDIT_CARD" -> maskCreditCard(value);
                    default -> mask(value);
                };
            });
        }

        return result;
    }

    /**
     * Mask sensitive fields in a JSON object.
     */
    public static String maskJsonFields(String json, Collection<String> fieldsToMask) {
        try {
            JsonNode rootNode = objectMapper.readTree(json);
            maskFields(rootNode, fieldsToMask);
            return objectMapper.writeValueAsString(rootNode);
        } catch (Exception e) {
            logger.error("Failed to mask JSON fields", e);
            return json;
        }
    }

    /**
     * Mask all sensitive fields in a JSON object using default sensitive field names.
     */
    public static String maskSensitiveFields(String json) {
        return maskJsonFields(json, SENSITIVE_FIELD_NAMES);
    }

    private static void maskFields(JsonNode node, Collection<String> fieldsToMask) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            Iterator<String> fieldNames = objectNode.fieldNames();

            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode childNode = objectNode.get(fieldName);

                if (shouldMaskField(fieldName, fieldsToMask)) {
                    if (childNode.isTextual()) {
                        String maskedValue = mask(childNode.asText());
                        objectNode.put(fieldName, maskedValue);
                    } else if (!childNode.isNull()) {
                        objectNode.put(fieldName, "****");
                    }
                } else {
                    maskFields(childNode, fieldsToMask);
                }
            }
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                maskFields(element, fieldsToMask);
            }
        }
    }

    private static boolean shouldMaskField(String fieldName, Collection<String> fieldsToMask) {
        String lowerFieldName = fieldName.toLowerCase();
        for (String sensitiveField : fieldsToMask) {
            if (lowerFieldName.contains(sensitiveField.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Add custom PII pattern.
     */
    public static void addPIIPattern(String name, Pattern pattern) {
        PII_PATTERNS.put(name, pattern);
    }

    /**
     * Add custom sensitive field name.
     */
    public static void addSensitiveFieldName(String fieldName) {
        SENSITIVE_FIELD_NAMES.add(fieldName);
    }

    /**
     * Get all registered PII patterns.
     */
    public static Set<String> getPIIPatternNames() {
        return new HashSet<>(PII_PATTERNS.keySet());
    }

    /**
     * Get all sensitive field names.
     */
    public static Set<String> getSensitiveFieldNames() {
        return new HashSet<>(SENSITIVE_FIELD_NAMES);
    }
}
