package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class KustoSinkTaskTest {
    File currentDirectory;

    @Before
    public final void before() {
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                GZIPFileWriter.class.getName(),
                String.valueOf(Instant.now().toEpochMilli())
        ).toString());
    }

    @After
    public final void after() {
        currentDirectory.delete();
    }

    @Test
    public void testSinkTaskOpen() throws Exception {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL, "https://{cluster_name}.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_DB, "db1");

        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_USERNAME, "test@test.com");
        props.put(KustoSinkConfig.KUSTO_AUTH_PASSWORD, "123456!");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        kustoSinkTask.start(props);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic1", 2));
        tps.add(new TopicPartition("topic2", 1));

        kustoSinkTask.open(tps);

        assertEquals(kustoSinkTask.writers.size(), 3);
    }

    @Test
    public void testSinkTaskPutRecord() throws Exception {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL, "https://{cluster_name}.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_DB, "db1");
        props.put(KustoSinkConfig.KUSTO_SINK_TEMPDIR, System.getProperty("java.io.tmpdir"));
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'testing1','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_USERNAME, "test@test.com");
        props.put(KustoSinkConfig.KUSTO_AUTH_PASSWORD, "123456!");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        kustoSinkTask.start(props);

        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition tp = new TopicPartition("topic1", 1);
        tps.add(tp);

        kustoSinkTask.open(tps);

        List<SinkRecord> records = new ArrayList<SinkRecord>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), 10));

        kustoSinkTask.put(records);

        assertEquals(kustoSinkTask.writers.get(tp).currentOffset, 10);
    }

    @Test
    public void testSinkTaskPutRecordMissingPartition() throws Exception {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL, "https://{cluster_name}.kusto.windows.net");

        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_USERNAME, "test@test.com");
        props.put(KustoSinkConfig.KUSTO_AUTH_PASSWORD, "123456!");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        kustoSinkTask.start(props);

        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));

        kustoSinkTask.open(tps);

        List<SinkRecord> records = new ArrayList<SinkRecord>();

        records.add(new SinkRecord("topic2", 1, null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), 10));

        Throwable exception = assertThrows(ConnectException.class, () -> kustoSinkTask.put(records));

        assertEquals(exception.getMessage(), "Received a record without a mapped writer for topic:partition(topic2:1), dropping record.");

    }

    @Test
    public void sinkStartMissingUrlOrDbOrTables() {
        HashMap<String, String> props = new HashMap<>();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();

        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Missing required configuration \"kusto.url\" which has no default value.");

        }

        props.put(KustoSinkConfig.KUSTO_URL, "https://{cluster_name}.kusto.windows.net");

        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Malformed topics to kusto ingestion props mappings");
        }


        props.remove(KustoSinkConfig.KUSTO_TABLE);
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "[{'topic': 'testing1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'testing1','db': 'db1', 'table': 'table1','format': 'json','mapping': 'Mapping'}]");
        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Kusto authentication method must be provided.");
        }

        // check malformed table mapping throws properly
        props.remove(KustoSinkConfig.KUSTO_TABLES_MAPPING);
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "topic1");
        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Error trying to parse kusto ingestion props A JSONArray text must start with '[' at character 1");
        }
    }

    @Test
    public void sinkStartMissingAuth() {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL, "https://{cluster_name}.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_DB, "db1");
        props.put(KustoSinkConfig.KUSTO_TABLE, "table3");
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "[{'topic': 'testing1','db': 'db1', 'table': 'table1','format': 'csv'}]");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();

        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Kusto authentication method must be provided.");

        }

        props.put(KustoSinkConfig.KUSTO_AUTH_USERNAME, "test@test.com");

        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Kusto authentication missing Password.");

        }

        props.put(KustoSinkConfig.KUSTO_AUTH_PASSWORD, "123456!");

        {
            // should not throw any errors
            kustoSinkTask.start(props);
            assertNotNull(kustoSinkTask.kustoIngestClient);
        }

        props.remove(KustoSinkConfig.KUSTO_AUTH_USERNAME);
        props.remove(KustoSinkConfig.KUSTO_AUTH_PASSWORD);

        props.put(KustoSinkConfig.KUSTO_AUTH_APPID, "appid");

        {
            Throwable exception = assertThrows(ConnectException.class, () -> {
                kustoSinkTask.start(props);
            });

            assertEquals(exception.getMessage(), "Kusto Connector failed to start due to configuration error. Kusto authentication missing App Key.");

        }

        props.put(KustoSinkConfig.KUSTO_AUTH_APPKEY, "appkey");

        {
            // should not throw any errors
            kustoSinkTask.start(props);
            assertNotNull(kustoSinkTask.kustoIngestClient);
        }
    }


    @Test
    public void getTable() {
        HashMap<String, String> props = new HashMap<>();
        props.put(KustoSinkConfig.KUSTO_URL, "https://{cluster_name}.kusto.windows.net");
        props.put(KustoSinkConfig.KUSTO_TABLES_MAPPING, "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db2', 'table': 'table2','format': 'json','mapping': 'Mapping'}]");
        props.put(KustoSinkConfig.KUSTO_AUTH_USERNAME, "test@test.com");
        props.put(KustoSinkConfig.KUSTO_AUTH_PASSWORD, "123456!");

        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        kustoSinkTask.start(props);
        {
            // single table mapping should cause all topics to be mapped to a single table
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic1").getDatabaseName(), "db1");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic1").getTableName(), "table1");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic1").getAdditionalProperties().get("format"), "csv");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic2").getDatabaseName(), "db2");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic2").getTableName(), "table2");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic2").getAdditionalProperties().get("format"), "json");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic2").getAdditionalProperties().get("jsonMappingReference"), "Mapping");
            Assert.assertEquals(kustoSinkTask.getIngestionProps("topic3"), null);
        }
    }
}
