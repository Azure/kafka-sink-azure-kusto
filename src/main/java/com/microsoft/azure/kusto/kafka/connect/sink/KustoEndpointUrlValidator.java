package com.microsoft.azure.kusto.kafka.connect.sink;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that Kusto endpoint URLs point to legitimate Azure Data Explorer domains.
 * This prevents SSRF attacks where attacker-controlled URLs could be used to exfiltrate
 * AAD authentication tokens.
 *
 * <p>The validator enforces that the hostname of the URL ends with one of the known
 * Azure Data Explorer domain suffixes. An override flag can be set to bypass this
 * validation for scenarios such as Azure Private Link endpoints or development environments.
 */
public final class KustoEndpointUrlValidator {

    private static final Logger log = LoggerFactory.getLogger(KustoEndpointUrlValidator.class);

    /**
     * Known legitimate Azure Data Explorer domain suffixes.
     * These cover all Azure clouds and related services.
     */
    static final List<String> ALLOWED_DOMAIN_SUFFIXES = Collections.unmodifiableList(Arrays.asList(
            ".kusto.windows.net",              // Azure Public Cloud
            ".kustomfa.windows.net",           // Azure Public Cloud (MFA)
            ".kusto.chinacloudapi.cn",         // Azure China (21Vianet)
            ".kusto.cloudapi.de",              // Azure Germany (legacy)
            ".kusto.usgovcloudapi.net",        // Azure US Government
            ".kustodev.net",                   // Azure Dev/Test
            ".kusto.fabric.microsoft.com",     // Microsoft Fabric
            ".playfab.com",                    // PlayFab (backed by Kusto)
            ".azuresynapse.net"                // Azure Synapse Data Explorer pools
    ));

    private KustoEndpointUrlValidator() {
        // Utility class
    }

    /**
     * Validates that a URL points to a legitimate Azure Data Explorer endpoint.
     *
     * @param url       the URL string to validate
     * @param configKey the configuration key name (used in error messages)
     * @throws ConfigException if the URL is malformed, not HTTPS, uses an IP address,
     *                         or does not match any allowed domain suffix
     */
    public static void validateKustoEndpointUrl(String url, String configKey) {
        if (url == null || url.trim().isEmpty()) {
            return;
        }

        url = url.trim();

        URL parsedUrl;
        try {
            parsedUrl = new URL(url);
        } catch (MalformedURLException e) {
            throw new ConfigException(configKey, url,
                    "Invalid URL format: " + e.getMessage());
        }

        String scheme = parsedUrl.getProtocol().toLowerCase(Locale.ROOT);
        if (!"https".equals(scheme)) {
            throw new ConfigException(configKey, url,
                    "Only HTTPS URLs are allowed for Kusto endpoints. Found scheme: " + scheme);
        }

        String host = parsedUrl.getHost();
        if (host == null || host.isEmpty()) {
            throw new ConfigException(configKey, url,
                    "URL must contain a valid hostname.");
        }

        String lowerHost = host.toLowerCase(Locale.ROOT);

        // Reject IP addresses (both IPv4 and IPv6)
        if (isIpAddress(lowerHost)) {
            throw new ConfigException(configKey, url,
                    "IP addresses are not allowed for Kusto endpoints. Use a fully qualified domain name.");
        }

        for (String suffix : ALLOWED_DOMAIN_SUFFIXES) {
            if (lowerHost.endsWith(suffix)) {
                return;
            }
        }

        throw new ConfigException(configKey, url,
                "URL does not point to a known Azure Data Explorer endpoint. "
                        + "The hostname must end with one of: " + ALLOWED_DOMAIN_SUFFIXES + ". "
                        + "If you are using Azure Private Link or a custom endpoint, "
                        + "set '" + KustoSinkConfig.KUSTO_SINK_DISABLE_URL_VALIDATION + "=true' to bypass this check.");
    }

    /**
     * Validates the URL if validation is not disabled.
     * When validation is disabled, logs a warning.
     *
     * @param url            the URL to validate
     * @param configKey      the config key name for error messages
     * @param skipValidation whether to skip validation (override flag)
     * @throws ConfigException if validation fails and is not skipped
     */
    public static void validateUrl(String url, String configKey, boolean skipValidation) {
        if (skipValidation) {
            log.warn("Kusto endpoint URL validation is disabled for '{}'. "
                    + "This is not recommended for production use. URL: {}", configKey, url);
            return;
        }
        validateKustoEndpointUrl(url, configKey);
    }

    /**
     * Checks whether the given hostname is an IP address (IPv4 or IPv6).
     */
    static boolean isIpAddress(String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }

        // IPv6 bracketed or raw
        if (host.startsWith("[") || host.contains(":")) {
            return true;
        }

        // IPv4: must be exactly 4 groups of 1-3 digits separated by dots
        String[] parts = host.split("\\.", -1);
        if (parts.length != 4) {
            return false;
        }
        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3) {
                return false;
            }
            for (int i = 0; i < part.length(); i++) {
                char c = part.charAt(i);
                if (c < '0' || c > '9') {
                    return false;
                }
            }
            int val = Integer.parseInt(part);
            if (val < 0 || val > 255) {
                return false;
            }
        }
        return true;
    }
}

