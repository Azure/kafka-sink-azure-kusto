package com.microsoft.azure.kusto.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link KustoEndpointUrlValidator} and URL validation in {@link KustoSinkConfig}.
 */
public class KustoEndpointUrlValidatorTest {

    private static final String CONFIG_KEY = "kusto.ingestion.url";

    // ======================== Valid URLs ========================

    @ParameterizedTest
    @ValueSource(strings = {
            "https://ingest-mycluster.kusto.windows.net",
            "https://mycluster.kusto.windows.net",
            "https://ingest-mycluster.eastus.kusto.windows.net",
            "https://INGEST-MYCLUSTER.KUSTO.WINDOWS.NET",             // case insensitive
            "https://mycluster.kusto.windows.net/",                    // trailing slash
            "https://mycluster.kusto.windows.net:443",                 // explicit port
            "https://mycluster.kusto.windows.net/some/path",           // with path
            "https://mycluster.kustomfa.windows.net",                  // MFA
            "https://mycluster.kusto.chinacloudapi.cn",                // Azure China
            "https://mycluster.kusto.cloudapi.de",                     // Azure Germany
            "https://mycluster.kusto.usgovcloudapi.net",               // US Gov
            "https://mycluster.kustodev.net",                          // Dev/Test
            "https://mycluster.kusto.fabric.microsoft.com",            // Fabric
            "https://mycluster.playfab.com",                           // PlayFab
            "https://mycluster.azuresynapse.net",                      // Synapse
            "https://ingest-mycluster.eastus2.kusto.windows.net",      // regional
    })
    public void shouldAcceptValidKustoUrls(String url) {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateKustoEndpointUrl(url, CONFIG_KEY));
    }

    // ======================== Invalid URLs ========================

    @Test
    public void shouldRejectNonKustoDomain() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://evil.attacker.com", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @Test
    public void shouldRejectHttpUrl() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("http://mycluster.kusto.windows.net", CONFIG_KEY));
        assertTrue(e.getMessage().contains("Only HTTPS URLs are allowed"));
    }

    @Test
    public void shouldRejectIpAddressV4() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://192.168.1.1", CONFIG_KEY));
        assertTrue(e.getMessage().contains("IP addresses are not allowed"));
    }

    @Test
    public void shouldRejectIpAddressV4WithPort() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://10.0.0.1:443", CONFIG_KEY));
        assertTrue(e.getMessage().contains("IP addresses are not allowed"));
    }

    @Test
    public void shouldRejectIpAddressV6() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://[::1]", CONFIG_KEY));
        assertTrue(e.getMessage().contains("IP addresses are not allowed"));
    }

    @Test
    public void shouldRejectLocalhost() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://localhost", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @Test
    public void shouldRejectLocalhostWithPort() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://localhost:9999", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @Test
    public void shouldRejectMalformedUrl() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("not-a-url", CONFIG_KEY));
        assertTrue(e.getMessage().contains("Invalid URL format"));
    }

    @Test
    public void shouldRejectDomainSpoofWithKustoSubstring() {
        // Attacker domain that contains kusto but is not a legitimate suffix
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://kusto.windows.net.evil.com", CONFIG_KEY));
        assertTrue(e.getMessage().contains("does not point to a known Azure Data Explorer endpoint"));
    }

    @Test
    public void shouldRejectFtpScheme() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("ftp://mycluster.kusto.windows.net", CONFIG_KEY));
        assertTrue(e.getMessage().contains("Only HTTPS URLs are allowed"));
    }

    @Test
    public void shouldRejectCloudMetadataEndpoint() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl("https://169.254.169.254", CONFIG_KEY));
        assertTrue(e.getMessage().contains("IP addresses are not allowed"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://evil.attacker.com/ingest",
            "https://172.26.0.1:9999/ingest",
            "https://internal-service.corp.net",
            "https://not-kusto.microsoft.com",
            "https://kusto-fake.windows.net",               // not *.kusto.windows.net
            "https://kusto.windows.net.attacker.com",        // subdomain spoof
    })
    public void shouldRejectVariousAttackerUrls(String url) {
        assertThrows(ConfigException.class,
                () -> KustoEndpointUrlValidator.validateKustoEndpointUrl(url, CONFIG_KEY));
    }

    // ======================== Null/Empty handling ========================

    @Test
    public void shouldAcceptNullUrl() {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateKustoEndpointUrl(null, CONFIG_KEY));
    }

    @Test
    public void shouldAcceptEmptyUrl() {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateKustoEndpointUrl("", CONFIG_KEY));
    }

    @Test
    public void shouldAcceptWhitespaceUrl() {
        assertDoesNotThrow(() -> KustoEndpointUrlValidator.validateKustoEndpointUrl("  ", CONFIG_KEY));
    }

    // ======================== IP address detection ========================

    @Test
    public void shouldDetectIpV4() {
        assertTrue(KustoEndpointUrlValidator.isIpAddress("192.168.1.1"));
        assertTrue(KustoEndpointUrlValidator.isIpAddress("10.0.0.1"));
        assertTrue(KustoEndpointUrlValidator.isIpAddress("127.0.0.1"));
        assertTrue(KustoEndpointUrlValidator.isIpAddress("169.254.169.254"));
    }

    @Test
    public void shouldDetectIpV6() {
        assertTrue(KustoEndpointUrlValidator.isIpAddress("[::1]"));
        assertTrue(KustoEndpointUrlValidator.isIpAddress("::1"));
        assertTrue(KustoEndpointUrlValidator.isIpAddress("fe80::1"));
    }

    @Test
    public void shouldNotDetectDomainAsIp() {
        assertFalse(KustoEndpointUrlValidator.isIpAddress("mycluster.kusto.windows.net"));
        assertFalse(KustoEndpointUrlValidator.isIpAddress("localhost"));
        assertFalse(KustoEndpointUrlValidator.isIpAddress("my-cluster.eastus.kusto.windows.net"));
    }

    @Test
    public void shouldHandleNullAndEmptyInIpCheck() {
        assertFalse(KustoEndpointUrlValidator.isIpAddress(null));
        assertFalse(KustoEndpointUrlValidator.isIpAddress(""));
    }

    @Test
    public void shouldNotDetectInvalidIpPatterns() {
        assertFalse(KustoEndpointUrlValidator.isIpAddress("..."));
        assertFalse(KustoEndpointUrlValidator.isIpAddress("1.2.3.4.5"));
        assertFalse(KustoEndpointUrlValidator.isIpAddress("999.999.999.999"));
        assertFalse(KustoEndpointUrlValidator.isIpAddress("1.2.3"));
        assertFalse(KustoEndpointUrlValidator.isIpAddress("1234"));
    }

    // ======================== Override flag ========================

    @Test
    public void shouldSkipValidationWhenDisabled() {
        // Should not throw even with an invalid URL when validation is disabled
        assertDoesNotThrow(() ->
                KustoEndpointUrlValidator.validateUrl("https://evil.attacker.com", CONFIG_KEY, true));
    }

    @Test
    public void shouldValidateWhenNotDisabled() {
        assertThrows(ConfigException.class, () ->
                KustoEndpointUrlValidator.validateUrl("https://evil.attacker.com", CONFIG_KEY, false));
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
    public void shouldRejectHttpIngestUrlInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, "http://mycluster.kusto.windows.net");
        assertThrows(ConfigException.class, () -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldAcceptValidUrlsInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        // Uses default valid URLs from setupConfigs
        assertDoesNotThrow(() -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldAcceptInvalidUrlWhenValidationDisabled() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, "https://evil.attacker.com");
        configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, "https://evil.attacker.com");
        configs.put(KustoSinkConfig.KUSTO_SINK_DISABLE_URL_VALIDATION, "true");
        assertDoesNotThrow(() -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldRejectInvalidUrlWhenValidationExplicitlyEnabled() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, "https://evil.attacker.com");
        configs.put(KustoSinkConfig.KUSTO_SINK_DISABLE_URL_VALIDATION, "false");
        assertThrows(ConfigException.class, () -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldRejectIpAddressInConfig() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, "https://172.26.0.1:9999/ingest");
        assertThrows(ConfigException.class, () -> new KustoSinkConfig(configs));
    }

    @Test
    public void shouldAcceptAllAzureCloudDomainsInConfig() {
        String[] validUrls = {
                "https://mycluster.kusto.windows.net",
                "https://mycluster.kustomfa.windows.net",
                "https://mycluster.kusto.chinacloudapi.cn",
                "https://mycluster.kusto.cloudapi.de",
                "https://mycluster.kusto.usgovcloudapi.net",
                "https://mycluster.kustodev.net",
                "https://mycluster.kusto.fabric.microsoft.com",
                "https://mycluster.playfab.com",
                "https://mycluster.azuresynapse.net",
        };

        for (String url : validUrls) {
            HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
            configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, url);
            configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, url);
            assertDoesNotThrow(() -> new KustoSinkConfig(configs),
                    "Should accept URL: " + url);
        }
    }

    // ======================== Defense-in-depth in KustoSinkTask ========================

    @Test
    public void shouldRejectInvalidUrlInConnectionStringBuilder() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_DISABLE_URL_VALIDATION, "true"); // bypass config validation
        KustoSinkConfig config = new KustoSinkConfig(configs);

        // Now try to create a connection string with a malicious URL
        // The defense-in-depth check is disabled here because the config flag says so
        assertDoesNotThrow(() ->
                KustoSinkTask.createKustoEngineConnectionString(config, "https://evil.attacker.com"));
    }

    @Test
    public void shouldValidateUrlInConnectionStringBuilderWhenEnabled() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        // URL validation is on (default)
        KustoSinkConfig config = new KustoSinkConfig(configs);

        // Defense-in-depth check should reject this
        assertThrows(ConfigException.class, () ->
                KustoSinkTask.createKustoEngineConnectionString(config, "https://evil.attacker.com"));
    }
}
