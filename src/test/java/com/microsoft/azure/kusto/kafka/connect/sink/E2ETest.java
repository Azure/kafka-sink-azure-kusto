package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.HttpProxyServerBootstrap;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Disabled("We don't want these tests running as part of the build or CI. Comment this line to test manually.")
public class E2ETest {
    private static final String testPrefix = "tmpKafkaE2ETest";
    private static final Logger log = LoggerFactory.getLogger(E2ETest.class);
    private static String appId;
    private static String appKey;
    private static String authority;
    private static String cluster;
    private static String database;
    private static String tableBaseName;
    private static HttpProxyServer proxy;
    private final String basePath = Paths.get("src/test/resources/", "testE2E").toString();

    @BeforeAll
    public static void beforeAll() {
        /*
         * Some scanners report global declarations as not handling exceptions and also not santizing names. Extracting these to beforeAll and then sanitizing
         * the names should fix this.
         */
        // App credentials are not used in the file name that are created. These paths hence need not be sanitized for
        // path traversal vulnerability.
        appId = getProperty("appId", null, false);
        appKey = getProperty("appKey", null, false);
        authority = getProperty("authority", null, false);
        cluster = getProperty("cluster", null, false);
        // The database and table names are used in the file name and hence are sanitized ( ../.. or ./dd/ etc)
        database = getProperty("database", "e2e", true); // used in file names. Sanitize the name
        String defaultTableName = String.format("%s%s", testPrefix, UUID.randomUUID().toString().replace('-', '_'));
        tableBaseName = getProperty("table", defaultTableName, true);// used in file names. Sanitize the name
        setupAndStartProxy();
    }

    private static String getProperty(String propertyName, String defaultValue, boolean sanitizePath) {
        String propertyValue = System.getProperty(propertyName, defaultValue);
        if (StringUtils.isNotEmpty(propertyValue)) {
            return sanitizePath ? FilenameUtils.normalize(propertyValue) : propertyValue;
        }
        throw new IllegalArgumentException(String.format("Property %s cannot be empty", propertyName));
    }

    @AfterAll
    public static void afterAll() {
        shutdownProxy();
    }

    private static void setupAndStartProxy() {
        HttpProxyServerBootstrap httpProxyServerBootstrap = DefaultHttpProxyServer.bootstrap()
                .withAllowLocalOnly(true) // only run on localhost
                .withAuthenticateSslClients(false); // we aren't checking client certs
        // Start the proxy server
        proxy = httpProxyServerBootstrap.start();
    }

    private static void shutdownProxy() {
        proxy.stop();
    }

    @Test
    public void testE2ECsv() throws URISyntaxException, DataClientException, DataServiceException {
        String dataFormat = "csv";
        IngestionMapping.IngestionMappingKind ingestionMappingKind = IngestionMapping.IngestionMappingKind.CSV;
        String mapping = "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"transform\":\"SourceLocation\", \"Ordinal\" : \"0\"}}," +
                "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Ordinal\":\"1\"}},";
        String[] messages = new String[] {"first field a,11", "first field b,22"};
        List<byte[]> messagesBytes = new ArrayList<>();
        messagesBytes.add(messages[0].getBytes());
        messagesBytes.add(messages[1].getBytes());
        long flushInterval = 100;
        if (!executeTest(dataFormat, ingestionMappingKind, mapping, messagesBytes, flushInterval, false, false)) {
            Assertions.fail("Test failed");
        }
    }

    @Test
    public void testE2EJson() throws URISyntaxException, DataClientException, DataServiceException {
        String dataFormat = "json";
        IngestionMapping.IngestionMappingKind ingestionMappingKind = IngestionMapping.IngestionMappingKind.JSON;
        String mapping = "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"Path\":\"$.ColA\"}}," +
                "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Path\":\"$.ColB\"}},";
        String[] messages = new String[] {"{'ColA': 'first field a', 'ColB': '11'}", "{'ColA': 'first field b', 'ColB': '22'}"};
        List<byte[]> messagesBytes = new ArrayList<>();
        messagesBytes.add(messages[0].getBytes());
        messagesBytes.add(messages[1].getBytes());
        long flushInterval = 100;

        if (!executeTest(dataFormat, ingestionMappingKind, mapping, messagesBytes, flushInterval, false, false)) {
            Assertions.fail("Test failed");
        }
    }


    @Test
    public void testE2EAvro() throws URISyntaxException, DataClientException, DataServiceException {
        String dataFormat = "avro";
        IngestionMapping.IngestionMappingKind ingestionMappingKind = IngestionMapping.IngestionMappingKind.AVRO;
        String mapping = "{\"column\": \"ColA\", \"Properties\":{\"Field\":\"XText\"}}," +
                "{\"column\": \"ColB\", \"Properties\":{\"Field\":\"RowNumber\"}}";
        long flushInterval = 300;
        List<byte[]> messagesBytes = new ArrayList<>();
        try {
            byte[] message = IOUtils.toByteArray(
                    Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("data.avro")));
            messagesBytes.add(message);
        } catch (IOException ex) {
            Assertions.fail("Error reading avro file in E2E test");
        }
        if (!executeTest(dataFormat, ingestionMappingKind, mapping, messagesBytes, flushInterval, true, true)) {
            Assertions.fail("Test failed");
        }
    }

    private boolean executeTest(String dataFormat, IngestionMapping.IngestionMappingKind ingestionMappingKind,
            String mapping, List<byte[]> messagesBytes, long flushInterval, boolean streaming,
            boolean useProxy)
            throws URISyntaxException, DataServiceException, DataClientException {
        String table = tableBaseName + dataFormat;
        String mappingReference = dataFormat + "Mapping";
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                String.format("https://%s.kusto.windows.net/", cluster),
                appId, appKey, authority);
        Client engineClient = ClientFactory.createClient(engineCsb);
        String basepath = Paths.get(basePath, dataFormat).toString();
        try {
            if (tableBaseName.startsWith(testPrefix)) {
                engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
                engineClient.execute(database, String.format(
                        ".alter table %s policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:05\", \"MaximumNumberOfItems\": 2, \"MaximumRawDataSizeMB\": 100}'",
                        table));
                if (streaming) {
                    engineClient.execute(database, ".clear database cache streamingingestion schema");
                }
            }
            engineClient.execute(database, String.format(".create table ['%s'] ingestion %s mapping '%s' " +
                    "'[" + mapping + "]'", table, dataFormat, mappingReference));

            TopicPartition tp = new TopicPartition("testPartition" + dataFormat, 11);
            IngestionProperties ingestionProperties = new IngestionProperties(database, table);
            long fileThreshold = 100;
            TopicIngestionProperties props = new TopicIngestionProperties();
            props.ingestionProperties = ingestionProperties;
            props.ingestionProperties.setDataFormat(dataFormat);
            props.ingestionProperties.setIngestionMapping(mappingReference, ingestionMappingKind);
            String kustoDmUrl = String.format("https://ingest-%s.kusto.windows.net", cluster);
            String kustoEngineUrl = String.format("https://%s.kusto.windows.net", cluster);
            Map<String, String> settings = getKustoConfigs(
                    kustoDmUrl, kustoEngineUrl, basepath, mappingReference, fileThreshold,
                    flushInterval, tp, dataFormat, table, streaming, useProxy);
            KustoSinkTask kustoSinkTask = new KustoSinkTask();
            kustoSinkTask.start(settings);
            kustoSinkTask.open(Collections.singletonList(tp));
            List<SinkRecord> records = new ArrayList<>();
            for (byte[] messageBytes : messagesBytes) {
                records.add(
                        new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, messageBytes, 10));
            }
            kustoSinkTask.put(records);
            // Streaming result should show immediately
            int timeoutMs = streaming ? 0 : 60 * 6 * 1000;
            validateExpectedResults(engineClient, 2, table, timeoutMs);
        } catch (InterruptedException e) {
            return false;
        } finally {
            try {
                Files.deleteIfExists(Paths.get(basepath));
                if (table.startsWith(testPrefix)) {
                    engineClient.execute(database, ".drop table " + table);
                }
            } catch (IOException ex) {
                log.warn("Clean up error deleting file {}", basepath);
            }
        }
        return true;
    }

    private void validateExpectedResults(Client engineClient, Integer expectedNumberOfRows, String table, int timeoutMs)
            throws InterruptedException, DataClientException, DataServiceException {
        String query = String.format("%s | count", table);
        KustoResultSetTable res = engineClient.execute(database, query).getPrimaryResults();
        res.next();
        int rowCount = res.getInt(0);
        int timeElapsedMs = 0;
        int sleepPeriodMs = 5 * 1000;
        while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
            Thread.sleep(sleepPeriodMs);
            res = engineClient.execute(database, query).getPrimaryResults();
            res.next();
            rowCount = res.getInt(0);
            timeElapsedMs += sleepPeriodMs;
        }
        Assertions.assertEquals(expectedNumberOfRows, rowCount);
        log.info("Successfully ingested {} records", expectedNumberOfRows);
    }

    private Map<String, String> getKustoConfigs(String clusterUrl, String engineUrl, String basePath,
            String tableMapping,
            long fileThreshold, long flushInterval, TopicPartition topic,
            String format,
            String table, boolean streaming, boolean useProxy) {
        Map<String, String> settings = new HashMap<>();
        settings.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, clusterUrl);
        settings.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, engineUrl);
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF,
                String.format("[{'topic': '%s','db': '%s', 'table': '%s','format': '%s', 'mapping':'%s' %s}]",
                        topic.topic(), database, table, format, tableMapping, streaming ? ",'streaming':true" : ""));
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, appId);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, appKey);
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, authority);
        settings.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, basePath);
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_SIZE_BYTES_CONF, String.valueOf(fileThreshold));
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, String.valueOf(flushInterval));
        if (useProxy) {
            settings.put(KustoSinkConfig.KUSTO_CONNECTION_PROXY_HOST, proxy.getListenAddress().getHostName());
            settings.put(KustoSinkConfig.KUSTO_CONNECTION_PROXY_PORT,
                    String.valueOf(proxy.getListenAddress().getPort()));
        }
        return settings;
    }
}
