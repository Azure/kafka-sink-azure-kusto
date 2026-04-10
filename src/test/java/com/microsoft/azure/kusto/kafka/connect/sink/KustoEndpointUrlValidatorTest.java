package com.microsoft.azure.kusto.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link KustoEndpointUrlValidator} and URL validation in {@link KustoSinkConfig}.
 *
 * <p>The validator delegates domain matching to the azure-kusto-java SDK's
 * {@code KustoTrustedEndpoints} (which reads {@code WellKnownKustoEndpoints.json}).
 */
public class KustoEndpointUrlValidatorTest {

    private static final String CONFIG_KEY = "kusto.ingestion.url";

    // ======================== Valid URLs ========================

    @ParameterizedTest
    @ValueSource(strings = {
            // Public cloud - with scheme
            "https://ingest-mycluster.kusto.windows.net",
            "https://mycluster.kusto.windows.net",
            "https://ingest-mycluster.eastus.kusto.windows.net",
            "https://INGEST-MYCLUSTER.KUSTO.WINDOWS.NET",             // case insensitive
            "https://mycluster.kusto.windows.net/",                    // trailing slash
            "https://mycluster.kusto.windows.net:443",                 // explicit port
            "https://mycluster.kusto.windows.net/some/path",           // with path
            "https://mycluster.kustomfa.windows.net",                  // MFA
            "https://ingest-mycluster.eastus2.kusto.windows.net",      // regional
            // Azure China
            "https://mycluster.kusto.chinacloudapi.cn",
            // US Gov
            "https://mycluster.kusto.usgovcloudapi.net",
            // Dev/Test
            "https://mycluster.kustodev.windows.net",
            // Fabric
            "https://mycluster.kusto.fabric.microsoft.com",
            // PlayFab
            "https://mycluster.playfab.com",
            // Synapse
            "https://mycluster.kusto.azuresynapse.net",
            // Additional SDK-recognized suffixes
            "https://mycluster.playfabapi.com",
            "https://mycluster.azureplayfab.com",
            "https://mycluster.kusto.data.microsoft.com",
            // Sovereign clouds (from SDK WellKnownKustoEndpoints.json)
            "https://mycluster.kusto.core.eaglex.ic.gov",
            "https://mycluster.kusto.core.microsoft.scloud",
            "https://mycluster.kusto.sovcloud-api.fr",
            "https://mycluster.kusto.sovcloud-api.de",
            "https://mycluster.kusto.sovcloud-api.sg",
    })
    public void shouldAcceptValidKustoUrls(String url) {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateEndpointUrl(url, CONFIG_KEY));
    }

    // ======================== URLs without scheme ========================

    @ParameterizedTest
    @ValueSource(strings = {
            "mycluster.kusto.windows.net",
            "ingest-mycluster.kusto.windows.net",
            "mycluster.eastus.kusto.windows.net",
            "mycluster.kusto.chinacloudapi.cn",
            "mycluster.kusto.usgovcloudapi.net",
            "mycluster.kusto.fabric.microsoft.com",
    })
    public void shouldAcceptUrlsWithoutScheme(String url) {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateEndpointUrl(url, CONFIG_KEY));
    }

    // ======================== Invalid URLs ========================

    @Test
    public void shouldRejectHttpUrl() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateEndpointUrl("http://mycluster.kusto.windows.net", CONFIG_KEY));
        assertTrue(e.getMessage().contains("HTTP is not supported"));
    }

    @Test
    public void shouldRejectNonKustoDomain() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateEndpointUrl("https://evil.attacker.com", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @Test
    public void shouldRejectNonKustoDomainWithoutScheme() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateEndpointUrl("evil.attacker.com", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @Test
    public void shouldRejectDomainSpoofWithKustoSubstring() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateEndpointUrl("https://kusto.windows.net.evil.com", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://evil.attacker.com/ingest",
            "https://internal-service.corp.net",
            "https://not-kusto.microsoft.com",
            "https://kusto-fake.windows.net",               // not *.kusto.windows.net
            "https://kusto.windows.net.attacker.com",        // subdomain spoof
    })
    public void shouldRejectVariousAttackerUrls(String url) {
        assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateEndpointUrl(url, CONFIG_KEY));
    }

    // ======================== Null/Empty handling ========================

    @Test
    public void shouldAcceptNullUrl() {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateEndpointUrl(null, CONFIG_KEY));
    }

    @Test
    public void shouldAcceptEmptyUrl() {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateEndpointUrl("", CONFIG_KEY));
    }

    @Test
    public void shouldAcceptWhitespaceUrl() {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateEndpointUrl("  ", CONFIG_KEY));
    }

    // ======================== Integration with KustoSinkConfig ========================

    @Test
    public void shouldRejectInvalidUrlInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, "https://evil.attacker.com");
        assertThrows(ConfigException.class, () -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldRejectInvalidEngineUrlInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, "https://evil.attacker.com");
        assertThrows(ConfigException.class, () -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldAcceptValidUrlsInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        // Uses default valid URLs from setupConfigs
        assertDoesNotThrow(() -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldAcceptUrlWithoutSchemeInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, "ingest-mycluster.kusto.windows.net");
        configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, "mycluster.kusto.windows.net");
        assertDoesNotThrow(() -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldAcceptPublicCloudDomainsInConfig() {
        String[] validUrls = {
                "https://mycluster.kusto.windows.net",
                "https://mycluster.kustomfa.windows.net",
                "https://mycluster.kusto.fabric.microsoft.com",
                "https://mycluster.playfab.com",
                "https://mycluster.kusto.azuresynapse.net",
                "https://mycluster.kustodev.windows.net",
        };

        for (String url : validUrls) {
            HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
            configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, url);
            configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, url);
            assertDoesNotThrow(() -> new KustoSinkConfig(configs),
                    "Should accept URL: " + url);
        }
    }

    @Test
    public void shouldAcceptSovereignCloudDomainsInConfig() {
        String[] validUrls = {
                "https://mycluster.kusto.chinacloudapi.cn",
                "https://mycluster.kusto.usgovcloudapi.net",
                "https://mycluster.kusto.core.eaglex.ic.gov",
                "https://mycluster.kusto.core.microsoft.scloud",
                "https://mycluster.kusto.sovcloud-api.fr",
                "https://mycluster.kusto.sovcloud-api.de",
                "https://mycluster.kusto.sovcloud-api.sg",
        };

        for (String url : validUrls) {
            HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
            configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, url);
            configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, url);
            assertDoesNotThrow(() -> new KustoSinkConfig(configs),
                    "Should accept sovereign cloud URL: " + url);
        }
    }

    // ======================== Defense-in-depth in KustoSinkTask ========================

    @Test
    public void shouldRejectInvalidUrlInConnectionStringBuilder() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkConfig config = new KustoSinkConfig(configs);

        // Defense-in-depth check should reject a malicious URL
        assertThrows(ConfigException.class, () ->
                KustoSinkTask.createKustoEngineConnectionString(config, "https://evil.attacker.com"));
    }

    @Test
    public void shouldAcceptValidUrlInConnectionStringBuilder() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkConfig config = new KustoSinkConfig(configs);

        // Valid Kusto URL should be accepted
        assertDoesNotThrow(() ->
                KustoSinkTask.createKustoEngineConnectionString(config, "https://mycluster.kusto.windows.net"));
    }
}