package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        config = new KustoSinkConfig(settings);
        assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        config = new KustoSinkConfig(settings);
        assertNotNull(config.getKustoUrl());
        assertNull(config.getTopicToTableMapping());
        assertNotNull(config.getFlushSizeBytes());
        assertNotNull(config.getFlushInterval());
        assertFalse(config.isDlqEnabled());
        assertEquals(BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        settings.remove(KustoSinkConfig.KUSTO_URL_CONF);
        config = new KustoSinkConfig(settings);
    }
    
    @Test(expected = ConfigException.class)
    public void shouldFailWhenErrorToleranceIncorrectlyConfigured() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        
        settings.put(KustoSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, "DummyValue");
        config = new KustoSinkConfig(settings);
    }
    
    @Test
    public void verifyDlqSettings() {
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF, "localhost:8081,localhost:8082");
        //settings.put(KustoSinkConfig.CONNECTOR_NAME_CONF, "KustoConnectorTest");
        config = new KustoSinkConfig(settings);
        
        assertTrue(config.isDlqEnabled());
        assertEquals(Arrays.asList("localhost:8081", "localhost:8082"), config.getDlqBootstrapServers());
        //assertEquals("KustoConnectorTest-error", config.getDlqTopicName());
    }    

}
