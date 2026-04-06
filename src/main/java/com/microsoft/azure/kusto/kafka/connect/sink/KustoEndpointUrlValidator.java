package com.microsoft.azure.kusto.kafka.connect.sink;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;

/**
 * Validates that Kusto endpoint URLs point to legitimate Azure Data Explorer domains.
 * This prevents SSRF attacks where attacker-controlled URLs could be used to exfiltrate
 * AAD authentication tokens.
 *
 * <p>Validation is delegated to the azure-kusto-java SDK's
 * {@link KustoTrustedEndpoints}, which uses the canonical {@code WellKnownKustoEndpoints.json}
 * resource as the source of truth for all trusted Azure Data Explorer endpoints across
 * all Azure clouds and sovereign regions. This ensures the connector stays aligned with
 * the SDK automatically, without needing to reimplement endpoint matching logic.
 *
 * <p>If the URL is provided without a scheme, {@code https://} is prepended automatically.
 */
public final class KustoEndpointUrlValidator {

    private static final Logger log = LoggerFactory.getLogger(KustoEndpointUrlValidator.class);
    private static final String HTTPS_SCHEME_PREFIX = "https://";

    private KustoEndpointUrlValidator() {
        // Utility class
    }

    /**
     * Validates that a URL points to a legitimate Azure Data Explorer endpoint.
     * <p>
     * If the URL does not contain a scheme, {@code https://} is prepended.
     * Validation is delegated to the SDK's {@link KustoTrustedEndpoints}.
     *
     * @param url       the URL string to validate
     * @param configKey the configuration key name (used in error messages)
     * @throws ConfigException if the URL does not match any known trusted Kusto endpoint
     */
    public static void validateEndpointUrl(String url, String configKey) {
        if (StringUtils.isBlank(url)) {
            return;
        }

        url = url.trim();

        // Prepend https:// if no scheme is provided (e.g. "mycluster.kusto.windows.net")
        if (!url.startsWith("https://") && !url.startsWith("http://")) {
            url = HTTPS_SCHEME_PREFIX + url;
        }

        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new ConfigException(configKey, url,
                    "Invalid URL format: " + e.getMessage());
        }

        // Delegate validation to the SDK's KustoTrustedEndpoints,
        // checking against all known Azure clouds and sovereign regions
        WellKnownKustoEndpointsData endpointsData = WellKnownKustoEndpointsData.getInstance();
        for (String loginEndpoint : endpointsData.AllowedEndpointsByLogin.keySet()) {
            try {
                KustoTrustedEndpoints.validateTrustedEndpoint(uri, loginEndpoint);
                return; // Trusted by this cloud
            } catch (KustoClientInvalidConnectionStringException e) {
                // Not trusted for this login endpoint, try next cloud
            }
        }

        throw new ConfigException(configKey, url,
                "URL does not point to a known Azure Data Explorer endpoint. "
                        + "The hostname must be a well-known trusted Kusto endpoint "
                        + "(see WellKnownKustoEndpoints.json in azure-kusto-java SDK).");
    }
}
