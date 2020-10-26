package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class KustoSinkConnectorConfigTest {
    private static final String DM_URL = "https://ingest-cluster_name.kusto.windows.net";
    private static final String ENGINE_URL = "https://cluster_name.kusto.windows.net";

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        KustoSinkConfig config = new KustoSinkConfig(setupConfigs());
        assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        KustoSinkConfig config = new KustoSinkConfig(setupConfigs());
        assertNotNull(config.getKustoUrl());
        assertNotEquals(0, config.getFlushSizeBytes());
        assertNotEquals(0, config.getFlushInterval());
        assertFalse(config.isDlqEnabled());
        assertEquals(BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(KustoSinkConfig.KUSTO_INGEST_URL_CONF);
        new KustoSinkConfig(settings);
    }

    @Test
    public void shouldUseKustoEngineUrlWhenGiven() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, ENGINE_URL);
        KustoSinkConfig config = new KustoSinkConfig(settings);
        String kustoEngineUrl = config.getKustoEngineUrl();
        assertEquals(ENGINE_URL, kustoEngineUrl);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenAppIdNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(KustoSinkConfig.KUSTO_AUTH_APPID_CONF);
        new KustoSinkConfig(settings);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWhenBehaviorOnErrorIsIllConfigured() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(KustoSinkConfig.KUSTO_INGEST_URL_CONF);
        settings.put(KustoSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, "DummyValue");
        new KustoSinkConfig(settings);
    }

    @Test
    public void verifyDlqSettings() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(KustoSinkConfig.KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF, "localhost:8081,localhost:8082");
        settings.put(KustoSinkConfig.KUSTO_DLQ_TOPIC_NAME_CONF, "dlq-error-topic");
        KustoSinkConfig config = new KustoSinkConfig(settings);

        Assertions.assertTrue(config.isDlqEnabled());
        assertEquals(Arrays.asList("localhost:8081", "localhost:8082"), config.getDlqBootstrapServers());
        assertEquals("dlq-error-topic", config.getDlqTopicName());
    }

    @Test
    public void shouldProcessDlqConfigsWithPrefix() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.put("misc.deadletterqueue.security.protocol", "SASL_PLAINTEXT");
        settings.put("misc.deadletterqueue.sasl.mechanism", "PLAIN");

        KustoSinkConfig config = new KustoSinkConfig(settings);

        Assertions.assertNotNull(config);

        Properties dlqProps = config.getDlqProps();

        assertEquals("SASL_PLAINTEXT", dlqProps.get("security.protocol"));
        assertEquals("PLAIN", dlqProps.get("sasl.mechanism"));
    }

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, DM_URL);
        configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, ENGINE_URL);
        configs.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db2', 'table': 'table2','format': 'json','mapping': 'Mapping'}]");
        configs.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        configs.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        configs.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        return configs;
    }
}