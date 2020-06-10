package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class KustoSinkConnectorConfigTest {
    Map<String, String> settings;
    KustoSinkConfig config;

    @Before
    public void before() {
        settings = new HashMap<>();
        settings.put(ConnectorConfig.NAME_CONFIG, "kusto-sink");
        config = null;
    }

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'kafka','db': 'Database', 'table': 'tableName','format': 'csv', 'mapping':'tableMapping'}]");
        config = new KustoSinkConfig(settings);
        assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'kafka','db': 'Database', 'table': 'tableName','format': 'csv', 'mapping':'tableMapping'}]");
        config = new KustoSinkConfig(settings);
        assertNotNull(config.getKustoUrl());
        assertNotNull(config.getTopicToTableMapping());
        assertNotNull(config.getFlushSizeBytes());
        assertNotNull(config.getFlushInterval());
        assertNull(config.getDlqBootstrapServers());
        assertNull(config.getDlqTopicName());
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        settings.remove(KustoSinkConfig.KUSTO_URL_CONF);
        config = new KustoSinkConfig(settings);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenTableMappingIsNull() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.remove(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF);
        config = new KustoSinkConfig(settings);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionWhenInvalidErrorTolerance() {
        // Adding required Configuration with no default value.
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, "kusto-url");
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'kafka','db': 'Database', 'table': 'tableName','format': 'csv', 'mapping':'tableMapping'}]");
        settings.put(KustoSinkConfig.KUSTO_SINK_ERROR_TOLERANCE_CONF, "test");
        config = new KustoSinkConfig(settings);
    }

}