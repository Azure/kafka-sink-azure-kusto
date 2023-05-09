package com.microsoft.azure.kusto.kafka.connect.sink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KustoSinkConnectorTest {

    @Test
    public void testStart() {
        KustoSinkConnector kustoSinkConnector = new KustoSinkConnector();
        Map<String, String> mockProps = new HashMap<>();
        mockProps.put("kusto.ingestion.url", "testValue");
        mockProps.put("kusto.query.url", "testValue");
        mockProps.put("aad.auth.appkey", "testValue");
        mockProps.put("aad.auth.appid", "testValue");
        mockProps.put("aad.auth.authority", "testValue");
        mockProps.put("kusto.tables.topics.mapping", "testValue");
        kustoSinkConnector.start(mockProps);
        Assertions.assertNotNull(kustoSinkConnector);
        Assertions.assertNotNull(kustoSinkConnector.config());
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.ingestion.url").name, "kusto.ingestion.url");
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.query.url").name, "kusto.query.url");
    }

    @Test
    public void testTaskConfigs() {
        KustoSinkConnector kustoSinkConnector = new KustoSinkConnector();

        Map<String, String> mockProps = new HashMap<>();
        mockProps.put("kusto.ingestion.url", "testValue");
        mockProps.put("kusto.query.url", "testValue");
        mockProps.put("aad.auth.appkey", "testValue");
        mockProps.put("aad.auth.appid", "testValue");
        mockProps.put("aad.auth.authority", "testValue");
        mockProps.put("kusto.tables.topics.mapping", "testValue");

        kustoSinkConnector.start(mockProps);

        List<Map<String, String>> mapList = kustoSinkConnector.taskConfigs(0);
        Assertions.assertNotNull(kustoSinkConnector);
        Assertions.assertNotNull(kustoSinkConnector.config());
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.ingestion.url").name, "kusto.ingestion.url");
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.query.url").name, "kusto.query.url");

    }

}
