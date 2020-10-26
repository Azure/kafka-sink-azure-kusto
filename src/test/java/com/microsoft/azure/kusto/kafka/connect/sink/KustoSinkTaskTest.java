package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class KustoSinkTaskTest {
    File currentDirectory;

    @BeforeEach
    public final void before() {
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                FileWriter.class.getName(),
                String.valueOf(Instant.now().toEpochMilli())
        ).toString());
    }

    @AfterEach
    public final void after() {
        currentDirectory.delete();
    }

    @Test
    public void testSinkTaskOpen() throws {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL_CONF, "https://cluster_name.kusto.windows.net");

        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        props.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(props);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic1", 2));
        tps.add(new TopicPartition("topic2", 1));

        kustoSinkTaskSpy.open(tps);

        assertEquals(3, kustoSinkTaskSpy.writers.size());
    }

    @Test
    public void testSinkTaskPutRecord() throws {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL_CONF, "https://cluster_name.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, System.getProperty("java.io.tmpdir"));
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'testing1','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        props.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(props);

        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition tp = new TopicPartition("topic1", 1);
        tps.add(tp);

        kustoSinkTaskSpy.open(tps);

        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), 10));

        kustoSinkTaskSpy.put(records);

        assertEquals(10, kustoSinkTaskSpy.writers.get(tp).currentOffset);
    }

    @Test
    public void testSinkTaskPutRecordMissingPartition() {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL_CONF, "https://cluster_name.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, System.getProperty("java.io.tmpdir"));
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        props.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(props);

        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));

        kustoSinkTaskSpy.open(tps);

        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord("topic2", 1, null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), 10));

        Throwable exception = assertThrows(ConnectException.class, () -> kustoSinkTaskSpy.put(records));

        assertEquals("Received a record without a mapped writer for topic:partition(topic2:1), dropping record.", exception.getMessage());

    }

    @Test
    public void getTable() {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL_CONF, "https://cluster_name.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db2', 'table': 'table2','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        props.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        props.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(props);
        {
            // single table mapping should cause all topics to be mapped to a single table
            Assertions.assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            Assertions.assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());
            Assertions.assertEquals("csv", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDataFormat());
            Assertions.assertEquals("db2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            Assertions.assertEquals("table2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            Assertions.assertEquals("json", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDataFormat());
            Assertions.assertEquals("Mapping", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getIngestionMapping().getIngestionMappingReference());
            Assertions.assertNull(kustoSinkTaskSpy.getIngestionProps("topic3"));
        }
    }
}