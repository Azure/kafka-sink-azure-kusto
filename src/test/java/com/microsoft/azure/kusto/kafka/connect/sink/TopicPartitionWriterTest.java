package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//TODO parts of this test needs to be re-formatted and may need rewriting
public class TopicPartitionWriterTest {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriterTest.class);

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
    private File currentDirectory;
    private String basePathCurrent;
    private boolean isDlqEnabled;
    private String dlqTopicName;
    private Producer<byte[], byte[]> kafkaProducer;
    private MockProducer<byte[], byte[]> dlqMockProducer;

    @BeforeAll
    public static void beforeClass() {
        propsCsv.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        propsCsv.ingestionProperties.setDataFormat(dataFormat);
    }

    @BeforeEach
    public final void before() {
        currentDirectory = Utils.getCurrentWorkingDirectory();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<>(properties);
        isDlqEnabled = false;
        dlqTopicName = null;
        dlqMockProducer = new MockProducer<>(
                true, new ByteArraySerializer(), new ByteArraySerializer());
        basePathCurrent = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        Map<String, String> settings = getKustoConfigs(basePathCurrent, fileThreshold, flushInterval);
        config = new KustoSinkConfig(settings);
    }

    @AfterEach
    public final void afterEach() {
        FileUtils.deleteQuietly(currentDirectory);
    }

    @Test
    public void testHandleRollFile() {
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        writer.handleRollFile(descriptor);
        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            log.error("Error running testHandleRollFile", e);
            fail(e);
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
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, config, isDlqEnabled, dlqTopicName, kafkaProducer);

        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;

        writer.handleRollFile(descriptor);
        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            log.error("Error running testHandleRollFile", e);
            fail(e);
        }

        Assertions.assertEquals(fileSourceInfoArgument.getValue().getFilePath(), descriptor.path);
        Assertions.assertEquals(TABLE, ingestionPropertiesArgumentCaptor.getValue().getTableName());
        Assertions.assertEquals(DATABASE, ingestionPropertiesArgumentCaptor.getValue().getDatabaseName());
        Assertions.assertEquals(1024, fileSourceInfoArgument.getValue().getRawSizeInBytes());
    }

    @Test
    public void testGetFilename() {
        try {
            TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
            File writerFile = new File(writer.getFilePath(null));
            Assertions.assertEquals("kafka_testPartition_11_0.CSV.gz", writerFile.getName());
        } catch (Exception ex) {
            // In case there is an accessor exception getting the file
            fail(ex);
        }
    }

    @Test
    public void testGetFilenameAfterOffsetChanges() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }
        try {
            File writerFile = new File(writer.getFilePath(null));
            Assertions.assertTrue(writerFile.exists());
            Assertions.assertEquals("kafka_testPartition_11_5.CSV.gz", (new File(writer.getFilePath(null))).getName());
        } catch (Exception ex) {
            // In case there is an accessor exception getting the file
            fail(ex);
        }
    }

    @Test
    public void testOpenClose() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        writer.open();
        writer.close();
    }

    @Test
    public void testWriteNonStringAndOffset() {
        // String db = "testdb1";
        // String table = "testtable1";
        //
        // TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);
        //
        // List<SinkRecord> records = new ArrayList<SinkRecord>();
        // DummyRecord dummyRecord1 = new DummyRecord(1, "a", (long) 2);
        // DummyRecord dummyRecord2 = new DummyRecord(2, "b", (long) 4);
        //
        // records.add(new SinkRecord("topic", 1, null, null, null, dummyRecord1, 10));
        // records.add(new SinkRecord("topic", 2, null, null, null, dummyRecord2, 3));
        // records.add(new SinkRecord("topic", 2, null, null, null, dummyRecord2, 4));
        //
        // for (SinkRecord record : records) {
        // writer.writeRecord(record);
        // }
        //
        // Assert.assertEquals(writer.getFilePath(), "kafka_testPartition_11_0");
    }

    @Test
    public void testWriteStringyValuesAndOffset() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);

        writer.open();
        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assertions.assertTrue((new File(writer.fileWriter.currentFile.path)).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 3, IngestionProperties.DataFormat.CSV.name()),
                (new File(writer.fileWriter.currentFile.path)).getName());
        writer.close();
    }

    @Test
    public void testWriteStringValuesAndOffset() throws IOException {
        String[] messages = new String[] {"stringy message", "another,stringy,message", "{'also':'stringy','sortof':'message'}"};

        // Expect to finish file after writing forth message cause of fileThreshold
        long fileThreshold2 = messages[0].length() + messages[1].length() + messages[2].length() + messages[2].length() - 1;
        Map<String, String> settings2 = getKustoConfigs(basePathCurrent, fileThreshold2, flushInterval);
        KustoSinkConfig config2 = new KustoSinkConfig(settings2);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config2, isDlqEnabled, dlqTopicName, kafkaProducer);

        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[0], 10));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[1], 13));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 14));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 15));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 16));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assertions.assertEquals(15, (long) writer.lastCommittedOffset);
        Assertions.assertEquals(16, writer.currentOffset);

        String currentFileName = writer.fileWriter.currentFile.path;
        Assertions.assertTrue(new File(currentFileName).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 15, IngestionProperties.DataFormat.CSV.name()),
                (new File(currentFileName)).getName());

        // Read
        writer.fileWriter.finishFile(false);
        Function<SourceFile, String> assertFileConsumer = FileWriterTest.getAssertFileConsumerFunction(messages[2] + "\n");
        assertFileConsumer.apply(writer.fileWriter.currentFile);
        writer.close();
    }

    @Test
    public void testWriteBytesValuesAndOffset() throws IOException {
        byte[] message = IOUtils.toByteArray(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("data.avro")));
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(message);
        // Expect to finish file with one record although fileThreshold is high
        long fileThreshold2 = 128;
        TopicIngestionProperties propsAvro = new TopicIngestionProperties();
        propsAvro.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        propsAvro.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
        Map<String, String> settings2 = getKustoConfigs(basePathCurrent, fileThreshold2, flushInterval);
        KustoSinkConfig config2 = new KustoSinkConfig(settings2);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsAvro, config2, isDlqEnabled, dlqTopicName, kafkaProducer);

        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, o.toByteArray(), 10));

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
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        TopicPartitionWriter spyWriter = spy(writer);

        spyWriter.open();
        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord record : records) {
            spyWriter.writeRecord(record);
        }
        // 2 records are waiting to be ingested - expect close to revoke them so that even after 5 seconds it won't ingest
        Assertions.assertNull(spyWriter.lastCommittedOffset);
        spyWriter.close();
        Assertions.assertNull(spyWriter.lastCommittedOffset);

        Thread.sleep(flushInterval + contextSwitchInterval);
        Assertions.assertNull(spyWriter.lastCommittedOffset);
    }

    @Test
    public void testSendFailedRecordToDlqError() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, true, "dlq.topic.name", kafkaProducer);
        TopicPartitionWriter spyWriter = spy(writer);
        // TODO this is to be re-worked
        kafkaProducer = mock(Producer.class);
        spyWriter.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        when(kafkaProducer.send(anyObject(), anyObject())).thenReturn(null);
        assertThrows(KafkaException.class, () -> spyWriter.sendFailedRecordToDlq(records.get(0)));
    }

    @Test
    public void testSendFailedRecordToDlqSuccess() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, true, "dlq.topic.name", dlqMockProducer);
        TopicPartitionWriter spyWriter = spy(writer);

        spyWriter.open();

        SinkRecord testSinkRecord = new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4);

        byte[] recordKey = String.format("Failed to write record to KustoDB with the following kafka coordinates, "
                + "topic=%s, partition=%s, offset=%s.",
                testSinkRecord.topic(),
                testSinkRecord.kafkaPartition(),
                testSinkRecord.kafkaOffset()).getBytes(StandardCharsets.UTF_8);
        byte[] recordValue = testSinkRecord.value().toString().getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>("dlq.topic.name", recordKey, recordValue);

        // when(kafkaProducer.send(dlqRecord,anyObject())).thenReturn(null);

        dlqMockProducer.send(dlqRecord);

        List<ProducerRecord<byte[], byte[]>> history = dlqMockProducer.history();

        List<ProducerRecord<byte[], byte[]>> expected = Collections.singletonList(dlqRecord);

        Assertions.assertEquals(expected, history);

    }

    private Map<String, String> getKustoConfigs(String basePath, long fileThreshold, long flushInterval) {
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
}
