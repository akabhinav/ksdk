package com.kafka.sdk.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Security configuration for Kafka connections.
 */
public class SecurityConfig {

    private final String securityProtocol;
    private final String saslMechanism;
    private final String saslJaasConfig;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;
    private final String sslKeyPassword;
    private final String sslEndpointIdentificationAlgorithm;
    private final boolean sslEnabled;

    private SecurityConfig(Builder builder) {
        this.securityProtocol = builder.securityProtocol;
        this.saslMechanism = builder.saslMechanism;
        this.saslJaasConfig = builder.saslJaasConfig;
        this.sslTruststoreLocation = builder.sslTruststoreLocation;
        this.sslTruststorePassword = builder.sslTruststorePassword;
        this.sslKeystoreLocation = builder.sslKeystoreLocation;
        this.sslKeystorePassword = builder.sslKeystorePassword;
        this.sslKeyPassword = builder.sslKeyPassword;
        this.sslEndpointIdentificationAlgorithm = builder.sslEndpointIdentificationAlgorithm;
        this.sslEnabled = builder.sslEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public String getSslKeyPassword() {
        return sslKeyPassword;
    }

    public String getSslEndpointIdentificationAlgorithm() {
        return sslEndpointIdentificationAlgorithm;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public Map<String, Object> toProperties() {
        Map<String, Object> props = new HashMap<>();

        if (sslEnabled || securityProtocol.contains("SSL") || securityProtocol.contains("SASL")) {
            props.put("security.protocol", securityProtocol);
        }

        if (saslMechanism != null && securityProtocol.contains("SASL")) {
            props.put("sasl.mechanism", saslMechanism);
        }

        if (saslJaasConfig != null) {
            props.put("sasl.jaas.config", saslJaasConfig);
        }

        if (sslTruststoreLocation != null) {
            props.put("ssl.truststore.location", sslTruststoreLocation);
        }

        if (sslTruststorePassword != null) {
            props.put("ssl.truststore.password", sslTruststorePassword);
        }

        if (sslKeystoreLocation != null) {
            props.put("ssl.keystore.location", sslKeystoreLocation);
        }

        if (sslKeystorePassword != null) {
            props.put("ssl.keystore.password", sslKeystorePassword);
        }

        if (sslKeyPassword != null) {
            props.put("ssl.key.password", sslKeyPassword);
        }

        if (sslEndpointIdentificationAlgorithm != null) {
            props.put("ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
        }

        return props;
    }

    public static class Builder {
        private String securityProtocol = "PLAINTEXT";
        private String saslMechanism;
        private String saslJaasConfig;
        private String sslTruststoreLocation;
        private String sslTruststorePassword;
        private String sslKeystoreLocation;
        private String sslKeystorePassword;
        private String sslKeyPassword;
        private String sslEndpointIdentificationAlgorithm = "https";
        private boolean sslEnabled = false;

        public Builder securityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder saslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
            return this;
        }

        public Builder saslJaasConfig(String saslJaasConfig) {
            this.saslJaasConfig = saslJaasConfig;
            return this;
        }

        public Builder sslTruststoreLocation(String sslTruststoreLocation) {
            this.sslTruststoreLocation = sslTruststoreLocation;
            return this;
        }

        public Builder sslTruststorePassword(String sslTruststorePassword) {
            this.sslTruststorePassword = sslTruststorePassword;
            return this;
        }

        public Builder sslKeystoreLocation(String sslKeystoreLocation) {
            this.sslKeystoreLocation = sslKeystoreLocation;
            return this;
        }

        public Builder sslKeystorePassword(String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        public Builder sslKeyPassword(String sslKeyPassword) {
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        public Builder sslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
            this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
            return this;
        }

        public Builder sslEnabled(boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        public SecurityConfig build() {
            return new SecurityConfig(this);
        }
    }
}
