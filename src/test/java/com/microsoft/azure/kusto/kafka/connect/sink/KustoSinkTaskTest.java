package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
        boolean delete = currentDirectory.delete();
    }

    @Test
    public void testSinkTaskOpen() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic1", 2));
        tps.add(new TopicPartition("topic2", 1));

        kustoSinkTaskSpy.open(tps);

        assertEquals(3, kustoSinkTaskSpy.writers.size());
    }

    @Test
    public void testSinkTaskPutRecord() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(configs);

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
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, System.getProperty("java.io.tmpdir"));
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(configs);

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
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(configs);
        {
            // single table mapping should cause all topics to be mapped to a single table
            Assertions.assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            Assertions.assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());
            Assertions.assertEquals("csv", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDataFormat());
            Assertions.assertEquals("db2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            Assertions.assertEquals("table2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            Assertions.assertEquals("multijson",
                    kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDataFormat());
            Assertions.assertEquals("Mapping", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getIngestionMapping().getIngestionMappingReference());
            Assertions.assertNull(kustoSinkTaskSpy.getIngestionProps("topic3"));
        }
    }

    @Test
    public void closeTaskAndWaitToFinish() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic2", 2));
        tps.add(new TopicPartition("topic2", 3));
        kustoSinkTaskSpy.open(tps);

        // Clean fast close
        long l1 = System.currentTimeMillis();
        kustoSinkTaskSpy.close(tps);
        long l2 = System.currentTimeMillis();

        assertTrue(l2-l1 < 1000);

        // Check close time when one close takes time to close
        TopicPartition tp = new TopicPartition("topic2", 4);
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();

        props.ingestionProperties = kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties;

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, new KustoSinkConfig(configs), false, null, null);
        TopicPartitionWriter writerSpy = spy(writer);
        long sleepTime = 2 * 1000;
        Answer<Void> answer = invocation -> {
            Thread.sleep(sleepTime);
            return null;
        };

        doAnswer(answer).when(writerSpy).close();
        kustoSinkTaskSpy.open(tps);
        writerSpy.open();
        tps.add(tp);
        kustoSinkTaskSpy.writers.put(tp, writerSpy);

        kustoSinkTaskSpy.close(tps);
        long l3 = System.currentTimeMillis();
        System.out.println("l3-l2 " + (l3-l2));
        assertTrue(l3-l2 > sleepTime  && l3-l2 < sleepTime + 1000);
    }

    @Test
    public void precommitDoesntCommitNewerOffsets() throws InterruptedException {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.<KustoSinkConfig>any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition topic1 = new TopicPartition("topic1", 1);
        tps.add(topic1);
        kustoSinkTaskSpy.open(tps);
        TopicPartitionWriter topicPartitionWriter = kustoSinkTaskSpy.writers.get(topic1);
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties;
        TopicPartitionWriter topicPartitionWriterSpy = spy(new TopicPartitionWriter(topic1, mockedClient, props, new KustoSinkConfig(configs), false, null, null));
        topicPartitionWriterSpy.open();
        kustoSinkTaskSpy.writers.put(topic1,topicPartitionWriterSpy);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Stopped stoppedObj = new Stopped();
        AtomicInteger offset =  new AtomicInteger(1);
        Runnable insertExec = () -> {
            while (!stoppedObj.stopped) {
                List<SinkRecord> records = new ArrayList<>();

                records.add(new SinkRecord("topic1", 1, null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), offset.getAndIncrement()));
                kustoSinkTaskSpy.put(new ArrayList<>(records));
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignore) {
                }
            }
        };
        Future<?> runner = executor.submit(insertExec);
        Thread.sleep(500);
        stoppedObj.stopped = true;
        runner.cancel(true);
        int current = offset.get();
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topic1, new OffsetAndMetadata(current));

        Map<TopicPartition, OffsetAndMetadata> returnedOffsets = kustoSinkTaskSpy.preCommit(offsets);
        kustoSinkTaskSpy.close(tps);

        // Decrease one cause preCommit adds one
        Assertions.assertEquals(returnedOffsets.get(topic1).offset() - 1,topicPartitionWriterSpy.lastCommittedOffset);
        Thread.sleep(500);
        // No ingestion occur even after waiting
        Assertions.assertEquals(returnedOffsets.get(topic1).offset() - 1,topicPartitionWriterSpy.lastCommittedOffset);
    }

    static class Stopped {
        boolean stopped;
    }
}