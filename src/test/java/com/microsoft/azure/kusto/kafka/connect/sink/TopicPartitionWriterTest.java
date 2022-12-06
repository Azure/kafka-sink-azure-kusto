package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.Mockito.*;

public class TopicPartitionWriterTest {
    private static final String KUSTO_INGEST_CLUSTER_URL = "https://ingest-cluster.kusto.windows.net";
    private static final String KUSTO_CLUSTER_URL = "https://cluster.kusto.windows.net";
    private static final String DATABASE = "testdb1";
    private static final String TABLE = "testtable1";
    private static final long fileThreshold = 100;
    private static final long flushInterval = 5000;
    private static final IngestClient mockClient = mock(IngestClient.class);
    private static final TopicIngestionProperties propsCsv = new TopicIngestionProperties();
    private static final TopicPartition tp = new TopicPartition("testPartition", 11);
    private static final long contextSwitchInterval = 200;
    private static final IngestionProperties.DataFormat dataFormat = IngestionProperties.DataFormat.CSV;
    private static KustoSinkConfig config;
    // TODO: should probably find a better way to mock internal class (FileWriter)...
    private static File currentDirectory;
    private static String basePathCurrent;
    private static boolean isDlqEnabled;
    private static String dlqTopicName;
    @Mock
    private Producer<byte[], byte[]> dlqErrorMockProducer;
    private TopicPartitionWriter topicPartitionWriter;
    private AutoCloseable closeable;

    @BeforeAll
    public static void beforeClass() {
        propsCsv.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        propsCsv.ingestionProperties.setDataFormat(dataFormat);
        isDlqEnabled = false;
        dlqTopicName = "dlq.topic";
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                FileWriter.class.getSimpleName(),
                String.valueOf(Instant.now().toEpochMilli())).toString());
        basePathCurrent = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        Map<String, String> settings = getKustoConfigs(basePathCurrent, fileThreshold);
        config = new KustoSinkConfig(settings);

    }

    @AfterAll
    public static void afterAll() {
        try {
            FileUtils.deleteDirectory(currentDirectory);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Send some sink records. Make sure that this fails in ingestion. When this fails ingestion then, we have to verify that interaction happens with DLQ
     * producer. This will again have 2 scenarios, DLQ send fails and it succeeds
     */
    /*
     * @Test public void testSendFailedRecordToDlqError() throws InterruptedException { }
     *
     * @Test public void testSendFailedRecordToDlqSuccess() throws InterruptedException { List<SinkRecord> records = new ArrayList<>(); records.add(new
     * SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5)); records.add(new SinkRecord(tp.topic(),
     * tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4)); doThrow(new
     * ConnectException("Exception writing data")).when(topicPartitionWriter).writeRecord(any()); for (SinkRecord record : records) {
     * topicPartitionWriter.writeRecord(record); } doNothing().when(dlqErrorMockProducer).send(any(), any()); // when(dlqErrorMockProducer.send(any(),
     * any())).re(new KafkaException("Error sending message to DLQ")); // 2 records are waiting to be ingested - expect close to revoke them so that even after
     * 5 seconds it won't ingest Assertions.assertNull(topicPartitionWriter.lastCommittedOffset); topicPartitionWriter.close(); verify(dlqErrorMockProducer,
     * times(2)).send(any(), any()); Thread.sleep(flushInterval + contextSwitchInterval); Assertions.assertNull(topicPartitionWriter.lastCommittedOffset); }
     */
    private static Map<String, String> getKustoConfigs(String basePath, long fileThreshold) {
        Map<String, String> settings = new HashMap<>();
        settings.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, KUSTO_INGEST_CLUSTER_URL);
        settings.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, KUSTO_CLUSTER_URL);
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, basePath);
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_SIZE_BYTES_CONF, String.valueOf(fileThreshold));
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, String.valueOf(flushInterval));
        return settings;
    }

    @BeforeEach
    void initializeMocksForRuns() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, dlqErrorMockProducer);
        topicPartitionWriter = spy(writer);
        closeable = MockitoAnnotations.openMocks(this);
        topicPartitionWriter.open();
        // when(dlqErrorMockProducer.send(any(), any())).thenThrow(new KafkaException("Error sending message to DLQ"));
    }

    @AfterEach
    void closeMocks() throws Exception {
        topicPartitionWriter.close();
        closeable.close();
    }

    @Test
    public void testHandleRollFile() {
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, config, isDlqEnabled, dlqTopicName, dlqErrorMockProducer);
        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        descriptor.path = "somepath/somefile";
        writer.handleRollFile(descriptor);
        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            Assertions.fail("Ingest from file threw exception");
        }
        Assertions.assertEquals(fileSourceInfoArgument.getValue().getFilePath(), descriptor.path);
        Assertions.assertEquals(TABLE, ingestionPropertiesArgumentCaptor.getValue().getTableName());
        Assertions.assertEquals(DATABASE, ingestionPropertiesArgumentCaptor.getValue().getDatabaseName());
        Assertions.assertEquals(1024, fileSourceInfoArgument.getValue().getRawSizeInBytes());
    }

    @Test
    public void testHandleRollFileWithStreamingEnabled() {
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        props.streaming = true;
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, config, isDlqEnabled, dlqTopicName, dlqErrorMockProducer);
        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        descriptor.path = "somepath/somefile";
        writer.handleRollFile(descriptor);
        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            Assertions.fail("Ingest from file threw exception");
        }
        Assertions.assertEquals(fileSourceInfoArgument.getValue().getFilePath(), descriptor.path);
        Assertions.assertEquals(TABLE, ingestionPropertiesArgumentCaptor.getValue().getTableName());
        Assertions.assertEquals(DATABASE, ingestionPropertiesArgumentCaptor.getValue().getDatabaseName());
        Assertions.assertEquals(1024, fileSourceInfoArgument.getValue().getRawSizeInBytes());
    }

    @Test
    public void testGetFilename() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, dlqErrorMockProducer);
        Assertions.assertEquals("kafka_testPartition_11_0.CSV.gz", (new File(writer.getFilePath(null))).getName());
    }

    @Test
    public void testGetFilenameAfterOffsetChanges() {
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        for (SinkRecord record : records) {
            topicPartitionWriter.writeRecord(record);
        }
        Assertions.assertTrue((new File(topicPartitionWriter.getFilePath(null))).exists());
        Assertions.assertEquals("kafka_testPartition_11_5.CSV.gz", (new File(topicPartitionWriter.getFilePath(null))).getName());
    }

    @Test
    public void testOpenClose() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, dlqErrorMockProducer);
        writer.open();
        writer.close();
    }

    @Test
    public void testWriteStringyValuesAndOffset() {
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        for (SinkRecord record : records) {
            topicPartitionWriter.writeRecord(record);
        }
        Assertions.assertTrue((new File(topicPartitionWriter.fileWriter.currentFile.path)).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 3, IngestionProperties.DataFormat.CSV.name()),
                (new File(topicPartitionWriter.fileWriter.currentFile.path)).getName());
    }

    @Test
    public void testWriteStringValuesAndOffset() throws IOException {
        String[] messages = new String[]{"stringy message", "another,stringy,message", "{'also':'stringy','sortof':'message'}"};
        // Expect to finish file after writing forth message cause of fileThreshold
        long fileThreshold2 = messages[0].length() + messages[1].length() + messages[2].length() + messages[2].length() - 1;
        Map<String, String> settings2 = getKustoConfigs(basePathCurrent, fileThreshold2);
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[0], 10));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[1], 13));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 14));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 15));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 16));
        for (SinkRecord record : records) {
            topicPartitionWriter.writeRecord(record);
        }
        Assertions.assertEquals(15, (long) topicPartitionWriter.lastCommittedOffset);
        Assertions.assertEquals(16, topicPartitionWriter.currentOffset);
        String currentFileName = topicPartitionWriter.fileWriter.currentFile.path;
        Assertions.assertTrue(new File(currentFileName).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 15, IngestionProperties.DataFormat.CSV.name()),
                (new File(currentFileName)).getName());
        // Read
        topicPartitionWriter.fileWriter.finishFile(false);
        Function<SourceFile, String> assertFileConsumer = FileWriterTest.getAssertFileConsumerFunction(messages[2] + "\n");
        assertFileConsumer.apply(topicPartitionWriter.fileWriter.currentFile);
        topicPartitionWriter.close();
    }

    @Test
    public void testWriteBytesValuesAndOffset() throws IOException {
        byte[] dataRead = Files.readAllBytes(Paths.get("src/test/resources/data.avro"));
        // Expect to finish file with one record although fileThreshold is high
        long fileThreshold2 = 128;
        TopicIngestionProperties propsAvro = new TopicIngestionProperties();
        propsAvro.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        propsAvro.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
        Map<String, String> settings2 = getKustoConfigs(basePathCurrent, fileThreshold2);
        KustoSinkConfig config2 = new KustoSinkConfig(settings2);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsAvro, config2, isDlqEnabled, dlqTopicName, dlqErrorMockProducer);
        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, dataRead, 10));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assertions.assertEquals(10, (long) writer.lastCommittedOffset);
        Assertions.assertEquals(10, writer.currentOffset);
        String currentFileName = writer.fileWriter.currentFile.path;
        Assertions.assertTrue(new File(currentFileName).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 10, IngestionProperties.DataFormat.AVRO.name()),
                (new File(currentFileName)).getName());
        writer.close();

    }

    @Test
    public void testClose() throws InterruptedException {
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        for (SinkRecord record : records) {
            topicPartitionWriter.writeRecord(record);
        }
        // 2 records are waiting to be ingested - expect close to revoke them so that even after 5 seconds it won't ingest
        Assertions.assertNull(topicPartitionWriter.lastCommittedOffset);
        topicPartitionWriter.close();
        Thread.sleep(flushInterval + contextSwitchInterval);
        Assertions.assertNull(topicPartitionWriter.lastCommittedOffset);
    }
}
