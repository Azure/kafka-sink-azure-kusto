package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.appender.TestAppender;

import static com.microsoft.azure.kusto.kafka.connect.sink.Utils.getCurrentWorkingDirectory;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class KustoSinkTaskTest {
    File currentDirectory;

    @BeforeEach
    public final void before() {
        currentDirectory = getCurrentWorkingDirectory();
    }

    @AfterEach
    public final void after() {
        FileUtils.deleteQuietly(currentDirectory);
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
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
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
        try {
            HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
            configs.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, System.getProperty("java.io.tmpdir"));
            KustoSinkTask kustoSinkTask = new KustoSinkTask();
            KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
            doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
            kustoSinkTaskSpy.start(configs);
            ArrayList<TopicPartition> tps = new ArrayList<>();
            tps.add(new TopicPartition("topic1", 1));
            kustoSinkTaskSpy.open(tps);
            List<SinkRecord> records = new ArrayList<>();
            records.add(
                    new SinkRecord("topic2", 1, null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8),
                            10));
            Throwable exception = assertThrows(ConnectException.class, () -> kustoSinkTaskSpy.put(records));
            assertEquals("Received a record without a mapped writer for topic:partition(topic2:1), dropping record.",
                    exception.getMessage());
        } catch (Exception ex) {
            // Accessors to system property may fail with runtime faults.
            fail(ex);
        }
    }

    @Test
    public void getTable() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        {
            // single table mapping should cause all topics to be mapped to a single table
            assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.CSV, kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDataFormat());
            assertEquals("Mapping", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getIngestionMapping().getIngestionMappingReference());
            assertEquals("db2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            assertEquals("table2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.MULTIJSON, kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDataFormat());
            Assertions.assertNull(kustoSinkTaskSpy.getIngestionProps("topic3"));
        }
    }

    @Test
    public void getTableWithoutMapping() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        {
            // single table mapping should cause all topics to be mapped to a single table
            assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.CSV, kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDataFormat());
            assertEquals("db2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            assertEquals("table2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.MULTIJSON, kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDataFormat());
            Assertions.assertNull(kustoSinkTaskSpy.getIngestionProps("topic3"));
        }
    }

    @Test
    public void closeTaskAndWaitToFinish() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
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
        assertTrue(l2 - l1 < 1000);
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
        System.out.println("l3-l2 " + (l3 - l2));
        assertTrue(l3 - l2 > sleepTime && l3 - l2 < sleepTime + 1000);
    }

    @Test
    public void testCreateKustoEngineClient() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        KustoSinkConfig kustoSinkConfig = new KustoSinkConfig(configs);
        Client client = KustoSinkTask.createKustoEngineClient(kustoSinkConfig);
        Assertions.assertNotNull(client);
    }

    @Test
    public void testValidateTableMappingsConnectError() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        configs.put(KustoSinkConfig.KUSTO_SINK_ENABLE_TABLE_VALIDATION, "true");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        KustoSinkConfig kustoSinkConfig = new KustoSinkConfig(configs);
        assertThrows(ConnectException.class, () -> kustoSinkTask.validateTableMappings(kustoSinkConfig));
    }

    @Test
    public void testStopSuccess() throws IOException {
        // set-up
        // easy to set it this way than mock
        TopicPartition mockPartition = new TopicPartition("test-topic", 0);
        TopicPartitionWriter mockPartitionWriter = mock(TopicPartitionWriter.class);
        doNothing().when(mockPartitionWriter).close();
        IngestClient mockClient = mock(IngestClient.class);
        doNothing().when(mockClient).close();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        // There is no mutate constructor
        kustoSinkTask.writers = Collections.singletonMap(mockPartition, mockPartitionWriter);
        kustoSinkTask.kustoIngestClient = mockClient;

        // when
        kustoSinkTask.stop();

        // then
        verify(mockClient, times(1)).close();
        verify(mockPartitionWriter, times(1)).close();
    }

    @Test
    public void testStopWriterFailure() throws IOException {
        // set-up
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        try {
            Logger.getLogger(KustoSinkTask.class).error("Error closing kusto client");
        } finally {
            logger.removeAppender(appender);
            logger.removeAllAppenders();
        }
        // easy to set it this way than mock
        TopicPartition mockPartition = new TopicPartition("test-topic", 1);
        TopicPartitionWriter mockPartitionWriter = mock(TopicPartitionWriter.class);
        doThrow(RuntimeException.class).when(mockPartitionWriter).close();
        IngestClient mockClient = mock(IngestClient.class);
        doNothing().when(mockClient).close();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        // There is no mutate constructor
        kustoSinkTask.writers = Collections.singletonMap(mockPartition, mockPartitionWriter);
        kustoSinkTask.kustoIngestClient = mockClient;
        final List<LoggingEvent> log = appender.getLog();
        final LoggingEvent firstLogEntry = log.get(0);
        assertEquals(firstLogEntry.getLevel().toString(), Level.ERROR.toString());
        assertEquals(firstLogEntry.getMessage(), "Error closing kusto client");
    }

    @Test
    public void testStopSinkTaskFailure() throws IOException {
        // set-up
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        try {
            Logger.getLogger(KustoSinkTask.class).error("Error closing kusto client");
        } finally {
            logger.removeAppender(appender);
            logger.removeAllAppenders();
        }
        // easy to set it this way than mock
        TopicPartition mockPartition = new TopicPartition("test-topic", 2);
        TopicPartitionWriter mockPartitionWriter = mock(TopicPartitionWriter.class);
        doNothing().when(mockPartitionWriter).close();
        IngestClient mockClient = mock(IngestClient.class);
        doThrow(IOException.class).when(mockClient).close();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        // There is no mutate constructor
        kustoSinkTask.writers = Collections.singletonMap(mockPartition, mockPartitionWriter);
        kustoSinkTask.kustoIngestClient = mockClient;
        final List<LoggingEvent> log = appender.getLog();
        final LoggingEvent firstLogEntry = log.get(0);
        assertEquals(firstLogEntry.getLevel().toString(), Level.ERROR.toString());
        assertEquals(firstLogEntry.getMessage(), "Error closing kusto client");
    }

    @Test
    public void precommitDoesntCommitNewerOffsets() throws InterruptedException {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition topic1 = new TopicPartition("topic1", 1);
        tps.add(topic1);
        kustoSinkTaskSpy.open(tps);
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties;
        TopicPartitionWriter topicPartitionWriterSpy = spy(
                new TopicPartitionWriter(topic1, mockedClient, props, new KustoSinkConfig(configs), false, null, null));
        topicPartitionWriterSpy.open();
        kustoSinkTaskSpy.writers.put(topic1, topicPartitionWriterSpy);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Stopped stoppedObj = new Stopped();
        AtomicInteger offset = new AtomicInteger(1);
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
        assertEquals(returnedOffsets.get(topic1).offset() - 1, topicPartitionWriterSpy.lastCommittedOffset);
        Thread.sleep(500);
        // No ingestion occur even after waiting
        assertEquals(returnedOffsets.get(topic1).offset() - 1, topicPartitionWriterSpy.lastCommittedOffset);
    }

    static class Stopped {
        boolean stopped;
    }
}
