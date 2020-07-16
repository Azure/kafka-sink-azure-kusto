package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KustoSinkConnectorConfigTest {
    Map<String, String> settings;
    KustoSinkConfig config;

    @Before
    public void before() {
        settings = new HashMap<>();
        config = null;
    }

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        settings.put("kusto.tables.topics.mapping","[{'topic': 'xxx','db': 'xxx', 'table': 'xxx','format': 'avro', 'mapping':'avri'}]");
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        config = new KustoSinkConfig(settings);
        assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        settings.put("kusto.tables.topics.mapping","[{'topic': 'xxx','db': 'xxx', 'table': 'xxx','format': 'avro', 'mapping':'avri'}]");
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        config = new KustoSinkConfig(settings);
        assertNotNull(config.getKustoUrl());
        assertNotNull(config.getFlushSizeBytes());
        assertNotNull(config.getFlushInterval());
        assertFalse(config.isDlqEnabled());
        assertEquals(BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        settings.remove(KustoSinkConfig.KUSTO_URL_CONF);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        config = new KustoSinkConfig(settings);
    }
    
    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenAppIdNotGiven() {
        // Adding required Configuration with no default value.
        settings.remove(KustoSinkConfig.KUSTO_URL_CONF);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        config = new KustoSinkConfig(settings);
    }
    
    @Test(expected = ConfigException.class)
    public void shouldFailWhenBehaviorOnErrorIsIllConfigured() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(KustoSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, "DummyValue");
        config = new KustoSinkConfig(settings);
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
        
        assertTrue(config.isDlqEnabled());
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

        assertNotNull(config);

        Properties dlqProps = config.getDlqProps();

        assertEquals("SASL_PLAINTEXT", dlqProps.get("security.protocol"));
        assertEquals("PLAIN", dlqProps.get("sasl.mechanism"));
    }

}
