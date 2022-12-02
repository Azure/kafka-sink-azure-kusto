package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import static com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.KUSTO_SINK_ENABLE_TABLE_VALIDATION;

public class KustoSinkConnectorConfigTest {
    private static final String DM_URL = "https://ingest-cluster_name.kusto.windows.net";
    private static final String ENGINE_URL = "https://cluster_name.kusto.windows.net";

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        KustoSinkConfig config = new KustoSinkConfig(setupConfigs());
        Assertions.assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        KustoSinkConfig config = new KustoSinkConfig(setupConfigs());
        Assertions.assertNotNull(config.getKustoIngestUrl());
        Assertions.assertTrue(config.getFlushSizeBytes() > 0);
        Assertions.assertTrue(config.getFlushInterval() > 0);
        Assertions.assertFalse(config.isDlqEnabled());
        Assertions.assertEquals(BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(KustoSinkConfig.KUSTO_INGEST_URL_CONF);
        Assertions.assertThrows(ConfigException.class, () -> new KustoSinkConfig(settings));
    }

    @Test
    public void shouldUseKustoEngineUrlWhenGiven() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, ENGINE_URL);
        KustoSinkConfig config = new KustoSinkConfig(settings);
        String kustoEngineUrl = config.getKustoEngineUrl();
        Assertions.assertEquals(ENGINE_URL, kustoEngineUrl);
    }

    @Test
    public void shouldThrowExceptionWhenAppIdNotGivenForApplicationAuth() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.put(KustoSinkConfig.KUSTO_AUTH_STRATEGY_CONF, KustoSinkConfig.KustoAuthenticationStrategy.APPLICATION.name());
        settings.remove(KustoSinkConfig.KUSTO_AUTH_APPID_CONF);
        KustoSinkConfig config = new KustoSinkConfig(settings);
        // In the previous PR this behavior was changed. The default was to use APPLICATION auth, but it also permits
        // MI. In case of MI the App-ID/App-KEY became optional
        Assertions.assertThrows(ConfigException.class, () -> KustoSinkTask.createKustoEngineConnectionString(config, config.getKustoEngineUrl()));
    }

    @Test
    public void shouldNotThrowExceptionWhenAppIdNotGivenForManagedIdentity() {
        // Same test as above. In this case since the Auth method is MI AppId/Key becomes optional.
        HashMap<String, String> settings = setupConfigs();
        settings.put(KustoSinkConfig.KUSTO_AUTH_STRATEGY_CONF, KustoSinkConfig.KustoAuthenticationStrategy.MANAGED_IDENTITY.name());
        settings.remove(KustoSinkConfig.KUSTO_AUTH_APPID_CONF);
        KustoSinkConfig config = new KustoSinkConfig(settings);
        ConnectionStringBuilder kcsb = KustoSinkTask.createKustoEngineConnectionString(config, config.getKustoEngineUrl());
        Assertions.assertNotNull(kcsb);
    }

    @Test
    public void shouldFailWhenBehaviorOnErrorIsIllConfigured() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(KustoSinkConfig.KUSTO_INGEST_URL_CONF);
        settings.put(KustoSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, "DummyValue");
        Assertions.assertThrows(ConfigException.class, () -> new KustoSinkConfig(settings));
    }

    @Test
    public void verifyDlqSettings() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(KustoSinkConfig.KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF, "localhost:8081,localhost:8082");
        settings.put(KustoSinkConfig.KUSTO_DLQ_TOPIC_NAME_CONF, "dlq-error-topic");
        KustoSinkConfig config = new KustoSinkConfig(settings);
        Assertions.assertTrue(config.isDlqEnabled());
        Assertions.assertEquals(Arrays.asList("localhost:8081", "localhost:8082"), config.getDlqBootstrapServers());
        Assertions.assertEquals("dlq-error-topic", config.getDlqTopicName());
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
        Assertions.assertEquals("SASL_PLAINTEXT", dlqProps.get("security.protocol"));
        Assertions.assertEquals("PLAIN", dlqProps.get("sasl.mechanism"));
    }

    @Test
    public void shouldPerformTableValidationByDefault() {
        KustoSinkConfig config = new KustoSinkConfig(setupConfigs());
        Assertions.assertTrue(config.getEnableTableValidation());
    }

    @Test
    public void shouldPerformTableValidationBasedOnParameter() {
        Arrays.asList(Boolean.valueOf("true"), Boolean.valueOf("false")).forEach(enableValidation -> {
            HashMap<String, String> settings = setupConfigs();
            settings.put(KUSTO_SINK_ENABLE_TABLE_VALIDATION, enableValidation.toString());
            KustoSinkConfig config = new KustoSinkConfig(settings);
            Assertions.assertEquals(enableValidation, config.getEnableTableValidation());
        });
    }

    @Test
    public void shouldAllowNoPasswordIfManagedIdentityEnabled() {
        HashMap<String, String> settings = setupConfigs();
        settings.remove(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF);
        settings.put(KustoSinkConfig.KUSTO_AUTH_STRATEGY_CONF, KustoSinkConfig.KustoAuthenticationStrategy.MANAGED_IDENTITY.name().toLowerCase());
        KustoSinkConfig config = new KustoSinkConfig(settings);
        Assertions.assertNotNull(config);
    }

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, DM_URL);
        configs.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, ENGINE_URL);
        configs.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF,
                "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db2', 'table': 'table2','format': 'json','mapping': 'Mapping'}]");
        configs.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        configs.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        configs.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        return configs;
    }
}
