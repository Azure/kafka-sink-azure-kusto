package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.testng.Assert;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class E2ETest {
    private static final String testPrefix = "tmpKafkaE2ETest";
    private String appId = System.getProperty("appId");
    private String appKey = System.getProperty("appKey");
    private String authority = System.getProperty("authority");
    private String cluster = System.getProperty("cluster");
    private String database = System.getProperty("database");
    private String tableBaseName = System.getProperty("table", testPrefix + UUID.randomUUID().toString().replace('-', '_'));
    private String basePath = Paths.get("src/test/resources/", "testE2E").toString();
    private Logger log = Logger.getLogger(this.getClass().getName());

    @Test
    @Ignore
    public void testE2ECsv() throws URISyntaxException, DataClientException, DataServiceException {
        String table = tableBaseName + "csv";
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://%s.kusto.windows.net", cluster), appId, appKey, authority);
        Client engineClient = ClientFactory.createClient(engineCsb);

        if (tableBaseName.startsWith(testPrefix)) {
            engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
        }
        try {
            engineClient.execute(database, String.format(".create table ['%s'] ingestion csv mapping 'mappy' " +
                    "'[" +
                    "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"transform\":\"SourceLocation\"}}," +
                    "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Ordinal\":\"1\"}}," +
                    "]'", table));

            TopicPartition tp = new TopicPartition("testPartition", 11);

            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://ingest-%s.kusto.windows.net", cluster), appId, appKey, authority);
            IngestClient ingestClient = IngestClientFactory.createClient(csb);
            IngestionProperties ingestionProperties = new IngestionProperties(database, table);
            String[] messages = new String[]{"stringy message,1", "another,2"};

            // Expect to finish file after writing forth message cause of fileThreshold
            long fileThreshold = 100;
            long flushInterval = 100;
            TopicIngestionProperties props = new TopicIngestionProperties();
            props.ingestionProperties = ingestionProperties;
            props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
            props.ingestionProperties.setIngestionMapping("mappy", IngestionMapping.IngestionMappingKind.Csv);
            String KustoUrl = String.format("https://ingest-%s.kusto.windows.net", cluster);
            String basepath = Paths.get(basePath, "csv").toString();
            Map<String, String> settings = getKustoConfigs(KustoUrl, basepath, "mappy", fileThreshold, flushInterval);
            KustoSinkConfig config= new KustoSinkConfig(settings);

            TopicPartitionWriter writer = new TopicPartitionWriter(tp, ingestClient, props, config);
            writer.open();

            List<SinkRecord> records = new ArrayList<SinkRecord>();
            records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, messages[0].getBytes(), 10));
            records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, messages[0], 10));

            for (SinkRecord record : records) {
                writer.writeRecord(record);
            }

            validateExpectedResults(engineClient, 2, table);
        } catch (InterruptedException e) {
            Assert.fail("Test failed", e);

        } finally {
            if (table.startsWith(testPrefix)) {
                engineClient.execute(database, ".drop table " + table);
            }
        }
    }

    @Test
    @Ignore
    public void testE2EAvro() throws URISyntaxException, DataClientException, DataServiceException {
        String table = tableBaseName + "avro";
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://%s.kusto.windows.net", cluster), appId, appKey, authority);
        Client engineClient = ClientFactory.createClient(engineCsb);
        try {

            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://ingest-%s.kusto.windows.net", cluster), appId, appKey, authority);
            IngestClient ingestClient = IngestClientFactory.createClient(csb);
            if (tableBaseName.startsWith(testPrefix)) {
                engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
            }
            engineClient.execute(database, String.format(".create table ['%s'] ingestion avro mapping 'avri' " +
                    "'[" +
                    "{\"column\": \"ColA\", \"Properties\":{\"Field\":\"XText\"}}," +
                    "{\"column\": \"ColB\", \"Properties\":{\"Field\":\"RowNumber\"}}" +
                    "]'", table));

            IngestionProperties ingestionProperties = new IngestionProperties(database, table);

            TopicIngestionProperties props2 = new TopicIngestionProperties();
            props2.ingestionProperties = ingestionProperties;
            props2.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.avro);
            props2.ingestionProperties.setIngestionMapping("avri", IngestionMapping.IngestionMappingKind.Avro);
            TopicPartition tp2 = new TopicPartition("testPartition2", 11);

            String KustoUrl = String.format("https://ingest-%s.kusto.windows.net", cluster);
            String basepath = Paths.get(basePath, "avro").toString();
            long fileThreshold = 100;
            long flushInterval = 300000;
            Map<String, String> settings = getKustoConfigs(KustoUrl, basepath, "avri", fileThreshold, flushInterval);
            KustoSinkConfig config= new KustoSinkConfig(settings);
            TopicPartitionWriter writer2 = new TopicPartitionWriter(tp2, ingestClient, props2, config);
            writer2.open();
            List<SinkRecord> records2 = new ArrayList<SinkRecord>();

            FileInputStream fs = new FileInputStream("src/test/resources/data.avro");
            byte[] buffer = new byte[1184];
            if (fs.read(buffer) != 1184) {
                Assert.fail("Error while ");
            }
            records2.add(new SinkRecord(tp2.topic(), tp2.partition(), null, null, Schema.BYTES_SCHEMA, buffer, 10));
            for (SinkRecord record : records2) {
                writer2.writeRecord(record);
            }

            validateExpectedResults(engineClient, 2, table);
        } catch (InterruptedException | IOException e) {
            Assert.fail("Test failed", e);
        } finally {
            if (table.startsWith(testPrefix)) {
                engineClient.execute(database, ".drop table " + table);
            }
        }
    }

    private void validateExpectedResults(Client engineClient, Integer expectedNumberOfRows, String table) throws InterruptedException, DataClientException, DataServiceException {
        String query = String.format("%s | count", table);

        KustoResultSetTable res = engineClient.execute(database, query).getPrimaryResults();
        res.next();
        Integer timeoutMs = 60 * 6 * 1000;
        Integer rowCount = res.getInt(0);
        Integer timeElapsedMs = 0;
        Integer sleepPeriodMs = 5 * 1000;

        while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
            Thread.sleep(sleepPeriodMs);
            res = engineClient.execute(database, query).getPrimaryResults();
            res.next();
            rowCount = res.getInt(0);
            timeElapsedMs += sleepPeriodMs;
        }
        Assertions.assertEquals(rowCount, expectedNumberOfRows);
        this.log.info("Succesfully ingested " + expectedNumberOfRows + " records.");
    }

    private Map<String, String> getKustoConfigs(String clusterUrl, String basePath,String tableMapping, long fileThreshold,
                                                long flushInterval) {
        Map<String, String> settings = new HashMap<>();
        settings.put(KustoSinkConfig.KUSTO_URL_CONF, clusterUrl);
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