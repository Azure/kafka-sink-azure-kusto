package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KustoSinkConnectorConfigTest {
    Map<String, String> settings;
    KustoSinkConfig config;

    @BeforeEach
    public void before() {
        settings = new HashMap<>();
        config = null;
    }

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        config = new KustoSinkConfig(settings);
        Assertions.assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        config = new KustoSinkConfig(settings);
        Assertions.assertNotNull(config.getKustoUrl());
        Assertions.assertTrue(config.getFlushSizeBytes() > 0);
        Assertions.assertTrue(config.getFlushInterval() > 0);
        Assertions.assertFalse(config.isDlqEnabled());
        assertEquals(BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        settings.remove(KustoSinkConfig.KUSTO_URL_CONF);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        Assertions.assertThrows(ConfigException.class, () -> {
            new KustoSinkConfig(settings);
        });
    }

    @Test
    public void shouldThrowExceptionWhenAppIdNotGiven() {
        settings.remove(KustoSinkConfig.KUSTO_URL_CONF);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        Assertions.assertThrows(ConfigException.class, () -> {
            new KustoSinkConfig(settings);
        });
    }

    @Test
    public void shouldFailWhenBehaviorOnErrorIsIllConfigured() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(KustoSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, "DummyValue");
        Assertions.assertThrows(ConfigException.class, () -> {
            new KustoSinkConfig(settings);
        });
    }

    @Test
    public void verifyDlqSettings() {
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(KustoSinkConfig.KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF, "localhost:8081,localhost:8082");
        settings.put(KustoSinkConfig.KUSTO_DLQ_TOPIC_NAME_CONF, "dlq-error-topic");
        config = new KustoSinkConfig(settings);

        Assertions.assertTrue(config.isDlqEnabled());
        assertEquals(Arrays.asList("localhost:8081", "localhost:8082"), config.getDlqBootstrapServers());
        assertEquals("dlq-error-topic", config.getDlqTopicName());
    }

    @Test
    public void shouldProcessDlqConfigsWithPrefix() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");

        settings.put("misc.deadletterqueue.security.protocol", "SASL_PLAINTEXT");
        settings.put("misc.deadletterqueue.sasl.mechanism", "PLAIN");

        config = new KustoSinkConfig(settings);

        Assertions.assertNotNull(config);

        Properties dlqProps = config.getDlqProps();

        assertEquals("SASL_PLAINTEXT", dlqProps.get("security.protocol"));
        assertEquals("PLAIN", dlqProps.get("sasl.mechanism"));
    }
}