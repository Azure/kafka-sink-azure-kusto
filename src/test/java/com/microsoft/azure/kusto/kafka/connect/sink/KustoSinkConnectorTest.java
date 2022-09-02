package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.KUSTO_INGEST_URL_CONF;

public class KustoSinkConnectorTest {


    @Test
    public void testStart(){
        KustoSinkConnector kustoSinkConnector = new KustoSinkConnector();
        Map<String,String> mockProps = new HashMap<>();
        mockProps.put("kusto.ingestion.url","testValue");
        mockProps.put("kusto.query.url","testValue");
        mockProps.put("aad.auth.appkey","testValue");
        mockProps.put("aad.auth.appid", "testValue");
        mockProps.put("aad.auth.authority","testValue");
        mockProps.put("kusto.tables.topics.mapping","testValue");

        ConfigDef configDef = new ConfigDef();
        configDef.define(
                KUSTO_INGEST_URL_CONF,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                "connection-url",
                "groupName",
                1,
                ConfigDef.Width.MEDIUM,
                "display");

        try (MockedStatic<KustoSinkConfig> utilities = Mockito.mockStatic(KustoSinkConfig.class)) {
            utilities.when(() -> KustoSinkConfig.getConfig())
                    .thenReturn(configDef);

            kustoSinkConnector.start(mockProps);

            Assertions.assertNotNull(configDef.configKeys());
            Assertions.assertEquals(configDef.configKeys().size(),1);
        }


        Assertions.assertNotNull(kustoSinkConnector);
        Assertions.assertNotNull(kustoSinkConnector.config());
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.ingestion.url").name,"kusto.ingestion.url");
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.query.url").name,"kusto.query.url");
    }

    @Test
    public void testStartMissingConfig(){
        KustoSinkConnector kustoSinkConnector = new KustoSinkConnector();
        Map<String,String> mockProps = new HashMap<>();
        Assertions.assertThrows(ConfigException.class,()-> {
            kustoSinkConnector.start(mockProps);
        });
    }

    @Test
    public void testTaskConfigs(){
        KustoSinkConnector kustoSinkConnector = new KustoSinkConnector();

        Map<String,String> mockProps = new HashMap<>();
        mockProps.put("kusto.ingestion.url","testValue");
        mockProps.put("kusto.query.url","testValue");
        mockProps.put("aad.auth.appkey","testValue");
        mockProps.put("aad.auth.appid", "testValue");
        mockProps.put("aad.auth.authority","testValue");
        mockProps.put("kusto.tables.topics.mapping","testValue");

        kustoSinkConnector.start(mockProps);

        List<Map<String, String>> mapList = kustoSinkConnector.taskConfigs(0);
        Assertions.assertNotNull(kustoSinkConnector);
        Assertions.assertNotNull(kustoSinkConnector.config());
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.ingestion.url").name,"kusto.ingestion.url");
        Assertions.assertEquals(kustoSinkConnector.config().configKeys().get("kusto.query.url").name,"kusto.query.url");

    }

}
