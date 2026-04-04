package com.microsoft.azure.kusto.kafka.connect.sink;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.auth.endpoints.FastSuffixMatcher;
import com.microsoft.azure.kusto.data.auth.endpoints.MatchRule;
import com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData;

/**
 * Validates that Kusto endpoint URLs point to legitimate Azure Data Explorer domains.
 * This prevents SSRF attacks where attacker-controlled URLs could be used to exfiltrate
 * AAD authentication tokens.
 *
 * <p>The set of trusted endpoints is loaded from the azure-kusto-java SDK's
 * {@code WellKnownKustoEndpoints.json} resource, which is the canonical source of truth
 * for all Azure Data Explorer endpoints across all Azure clouds and sovereign regions.
 * This ensures the connector stays aligned with the SDK automatically.
 *
 * <p>In addition, this class enforces HTTPS-only and rejects IP address literals
 * as a defense-in-depth measure.
 *
 * <p>An override flag ({@code kusto.validation.url.disable=true}) can be set to bypass
 * validation for scenarios such as Azure Private Link endpoints or development environments.
 */
public final class KustoEndpointUrlValidator {

    private static final Logger log = LoggerFactory.getLogger(KustoEndpointUrlValidator.class);

    /**
     * A suffix matcher built from all trusted Kusto endpoint suffixes and hostnames
     * across all Azure clouds, loaded from the SDK's WellKnownKustoEndpoints.json.
     */
    private static final FastSuffixMatcher TRUSTED_ENDPOINT_MATCHER;

    static {
        TRUSTED_ENDPOINT_MATCHER = buildTrustedEndpointMatcher();
    }

    private KustoEndpointUrlValidator() {
        // Utility class
    }

    /**
     * Builds a {@link FastSuffixMatcher} from the SDK's {@link WellKnownKustoEndpointsData},
     * aggregating all trusted suffixes and hostnames across all login endpoints / Azure clouds.
     */
    private static FastSuffixMatcher buildTrustedEndpointMatcher() {
        WellKnownKustoEndpointsData endpointsData = WellKnownKustoEndpointsData.getInstance();
        List<MatchRule> allRules = new ArrayList<>();

        endpointsData.AllowedEndpointsByLogin.forEach((loginEndpoint, allowedEndpoints) -> {
            if (allowedEndpoints.AllowedKustoSuffixes != null) {
                for (String suffix : allowedEndpoints.AllowedKustoSuffixes) {
                    allRules.add(new MatchRule(suffix, false));
                }
            }
            if (allowedEndpoints.AllowedKustoHostnames != null) {
                for (String hostname : allowedEndpoints.AllowedKustoHostnames) {
                    allRules.add(new MatchRule(hostname, true));
                }
            }
        });

        if (allRules.isEmpty()) {
            throw new IllegalStateException("No trusted Kusto endpoints loaded from WellKnownKustoEndpoints.json. "
                    + "This indicates a problem with the azure-kusto-java SDK dependency.");
        }

        log.debug("Loaded {} trusted Kusto endpoint rules from WellKnownKustoEndpoints.json", allRules.size());
        return FastSuffixMatcher.create(allRules);
    }

    /**
     * Validates that a URL points to a legitimate Azure Data Explorer endpoint.
     * <p>
     * Performs the following checks in order:
     * <ol>
     *   <li>URL is well-formed</li>
     *   <li>Scheme is HTTPS</li>
     *   <li>Hostname is not an IP address literal</li>
     *   <li>Hostname is a well-known trusted Kusto endpoint (via the SDK's WellKnownKustoEndpoints.json)</li>
     * </ol>
     *
     * @param url       the URL string to validate
     * @param configKey the configuration key name (used in error messages)
     * @throws ConfigException if the URL is malformed, not HTTPS, uses an IP address,
     *                         or does not match any known trusted Kusto endpoint
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

        // Validate against the SDK's trusted endpoint list
        if (!TRUSTED_ENDPOINT_MATCHER.isMatch(lowerHost)) {
            throw new ConfigException(configKey, url,
                    "URL does not point to a known Azure Data Explorer endpoint. "
                            + "The hostname must be a well-known trusted Kusto endpoint "
                            + "(see WellKnownKustoEndpoints.json in azure-kusto-java SDK). "
                            + "If you are using Azure Private Link or a custom endpoint, "
                            + "set '" + KustoSinkConfig.KUSTO_SINK_DISABLE_URL_VALIDATION + "=true' to bypass this check.");
        }
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

