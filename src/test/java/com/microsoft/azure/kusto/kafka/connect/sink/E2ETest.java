package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

@Disabled("We don't want these tests running as part of the build or CI. Comment this line to test manually.")
public class E2ETest {
    private static final String testPrefix = "tmpKafkaE2ETest";
    private static final String appId = System.getProperty("appId");
    private static final String appKey = System.getProperty("appKey");
    private static final String authority = System.getProperty("authority");
    private static final String cluster = System.getProperty("cluster");
    private static final String database = System.getProperty("database");
    private static final String tableBaseName = System.getProperty("table", testPrefix + UUID.randomUUID().toString().replace('-', '_'));
    private final String basePath = Paths.get("src/test/resources/", "testE2E").toString();
    private final Logger log = Logger.getLogger(this.getClass().getName());
    private boolean isDlqEnabled;
    private String dlqTopicName;
    private Producer<byte[], byte[]> kafkaProducer;

    @BeforeEach
    public void setUp() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<>(properties);
        isDlqEnabled = false;
        dlqTopicName = null;
    }

    @Test
    public void testE2ECsv() throws URISyntaxException, DataClientException, DataServiceException {
        String dataFormat = "csv";
        IngestionMapping.IngestionMappingKind ingestionMappingKind = IngestionMapping.IngestionMappingKind.Csv;
        String mapping = "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"transform\":\"SourceLocation\"}}," +
                         "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Ordinal\":\"1\"}},";
        String[] messages = new String[]{"first field a,11", "first field b,22"};
        List<byte[]> messagesBytes = new ArrayList<>();
        messagesBytes.add(messages[0].getBytes());
        messagesBytes.add(messages[1].getBytes());
        long flushInterval = 100;

        if (!executeTest(dataFormat, ingestionMappingKind, mapping, messagesBytes, flushInterval)) {
            Assertions.fail("Test failed");
        }
    }

    @Test
    public void testE2EJson() throws URISyntaxException, DataClientException, DataServiceException {
        String dataFormat = "json";
        IngestionMapping.IngestionMappingKind ingestionMappingKind = IngestionMapping.IngestionMappingKind.Json;
        String mapping = "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"Path\":\"$.ColA\"}}," +
                         "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Path\":\"$.ColB\"}},";
        String[] messages = new String[]{"{'ColA': 'first field a', 'ColB': '11'}", "{'ColA': 'first field b', 'ColB': '22'}"};
        List<byte[]> messagesBytes = new ArrayList<>();
        messagesBytes.add(messages[0].getBytes());
        messagesBytes.add(messages[1].getBytes());
        long flushInterval = 100;

        if (!executeTest(dataFormat, ingestionMappingKind, mapping, messagesBytes, flushInterval)) {
            Assertions.fail("Test failed");
        }
    }

    @Test
    public void testE2EAvro() throws URISyntaxException, DataClientException, DataServiceException {
        String dataFormat = "avro";
        IngestionMapping.IngestionMappingKind ingestionMappingKind = IngestionMapping.IngestionMappingKind.Avro;
        String mapping = "{\"column\": \"ColA\", \"Properties\":{\"Field\":\"XText\"}}," +
                         "{\"column\": \"ColB\", \"Properties\":{\"Field\":\"RowNumber\"}}";
        byte[] message = new byte[1184];
        try {
            FileInputStream fs = new FileInputStream("src/test/resources/data.avro");
            if (fs.read(message) != 1184) {
                Assertions.fail("Error while ");
            }
        } catch (IOException e) {
            Assertions.fail("Test failed");
        }
        List<byte[]> messagesBytes = new ArrayList<>();
        messagesBytes.add(message);
        long flushInterval = 300000;

        if (!executeTest(dataFormat, ingestionMappingKind, mapping, messagesBytes, flushInterval)) {
            Assertions.fail("Test failed");
        }
    }

    private boolean executeTest(String dataFormat, IngestionMapping.IngestionMappingKind ingestionMappingKind, String mapping, List<byte[]> messagesBytes, long flushInterval) throws URISyntaxException, DataServiceException, DataClientException {
        String table = tableBaseName + dataFormat;
        String mappingReference = dataFormat + "Mapping";
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://%s.kusto.windows.net/", cluster), appId, appKey, authority);
        Client engineClient = ClientFactory.createClient(engineCsb);

        try {
            if (tableBaseName.startsWith(testPrefix)) {
                engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
            }
            engineClient.execute(database, String.format(".create table ['%s'] ingestion %s mapping '%s' " +
                    "'[" + mapping + "]'", table, dataFormat, mappingReference));

            TopicPartition tp = new TopicPartition("testPartition" + dataFormat, 11);
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://ingest-%s.kusto.windows.net", cluster), appId, appKey, authority);
            IngestClient ingestClient = IngestClientFactory.createClient(csb);
            IngestionProperties ingestionProperties = new IngestionProperties(database, table);

            long fileThreshold = 100;
            TopicIngestionProperties props = new TopicIngestionProperties();
            props.ingestionProperties = ingestionProperties;
            props.ingestionProperties.setDataFormat(dataFormat);
            props.ingestionProperties.setIngestionMapping(mappingReference, ingestionMappingKind);
            String kustoDmUrl = String.format("https://ingest-%s.kusto.windows.net", cluster);
            String kustoEngineUrl = String.format("https://%s.kusto.windows.net", cluster);
            String basepath = Paths.get(basePath, dataFormat).toString();
            Map<String, String> settings = getKustoConfigs(kustoDmUrl, kustoEngineUrl, basepath, mappingReference, fileThreshold, flushInterval);
            KustoSinkConfig config = new KustoSinkConfig(settings);
            TopicPartitionWriter writer = new TopicPartitionWriter(tp, ingestClient, props, config, isDlqEnabled, dlqTopicName, kafkaProducer);
            writer.open();

            List<SinkRecord> records = new ArrayList<>();
            for (byte[] messageBytes : messagesBytes) {
                records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, messageBytes, 10));
            }

            for (SinkRecord record : records) {
                writer.writeRecord(record);
            }

            validateExpectedResults(engineClient, 2, table);
        } catch (InterruptedException e) {
            return false;
        } finally {
            if (table.startsWith(testPrefix)) {
                engineClient.execute(database, ".drop table " + table);
            }
        }

        return true;
    }

    private void validateExpectedResults(Client engineClient, Integer expectedNumberOfRows, String table) throws InterruptedException, DataClientException, DataServiceException {
        String query = String.format("%s | count", table);

        KustoResultSetTable res = engineClient.execute(database, query).getPrimaryResults();
        res.next();
        int timeoutMs = 60 * 6 * 1000;
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
        Assertions.assertEquals(rowCount, expectedNumberOfRows);
        this.log.info("Successfully ingested " + expectedNumberOfRows + " records.");
    }

    private Map<String, String> getKustoConfigs(String clusterUrl, String engineUrl, String basePath, String tableMapping,
                                                long fileThreshold, long flushInterval) {
        Map<String, String> settings = new HashMap<>();
        settings.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, clusterUrl);
        settings.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, engineUrl);
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, tableMapping);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, appId);
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, appKey);
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, authority);
        settings.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, basePath);
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_SIZE_BYTES_CONF, String.valueOf(fileThreshold));
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, String.valueOf(flushInterval));
        return settings;
    }
}