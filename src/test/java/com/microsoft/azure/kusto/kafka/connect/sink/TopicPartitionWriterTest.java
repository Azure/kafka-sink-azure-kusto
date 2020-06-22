package com.microsoft.azure.kusto.kafka.connect.sink;

import com.google.common.base.Function;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class TopicPartitionWriterTest {
    // TODO: should probably find a better way to mock internal class (FileWriter)...
    private File currentDirectory;
    private KustoSinkConfig config;

    @Before
    public final void before() {
      config = new KustoSinkConfig(getProperties());
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                FileWriter.class.getSimpleName(),
                String.valueOf(Instant.now().toEpochMilli())
        ).toString());
    }

    @After
    public final void after() {
        try {
            FileUtils.deleteDirectory(currentDirectory);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHandleRollFile() {
        TopicPartition tp = new TopicPartition("testPartition", 11);
        IngestClient mockedClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;
        long flushInterval = 300000;
        IngestionProperties ingestionProperties = new IngestionProperties(db, table);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = ingestionProperties;
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, basePath, fileThreshold, flushInterval, config);

        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        descriptor.path = "somepath/somefile";
        descriptor.file = new File ("C://myfile.txt");
        writer.handleRollFile(descriptor);

        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertEquals(fileSourceInfoArgument.getValue().getFilePath(), descriptor.path);
        Assert.assertEquals(table, ingestionPropertiesArgumentCaptor.getValue().getTableName());
        Assert.assertEquals(db, ingestionPropertiesArgumentCaptor.getValue().getDatabaseName());
        Assert.assertEquals(fileSourceInfoArgument.getValue().getRawSizeInBytes(), 1024);
    }

    @Test
    public void testGetFilename() {
        TopicPartition tp = new TopicPartition("testTopic", 11);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;
        long flushInterval = 300000;
        TopicIngestionProperties props = new TopicIngestionProperties();

        props.ingestionProperties = new IngestionProperties(db, table);
        props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, props, basePath, fileThreshold, flushInterval, config);

        Assert.assertEquals(writer.getFilePath(null), Paths.get(basePath, "kafka_testTopic_11_0.csv.gz").toString());
    }

    @Test
    public void testGetFilenameAfterOffsetChanges() {
        TopicPartition tp = new TopicPartition("testTopic", 11);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;
        long flushInterval = 300000;
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(db, table);
        props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, props, basePath, fileThreshold, flushInterval, config);
        writer.open();
        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assert.assertEquals(writer.getFilePath(null), Paths.get(basePath, "kafka_testTopic_11_5.csv.gz").toString());
    }

    @Test
    public void testOpenClose() {
        TopicPartition tp = new TopicPartition("testPartition", 1);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;
        long flushInterval = 300000;
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(db, table);
        props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, props, basePath, fileThreshold, flushInterval, config);
        writer.open();
        writer.close();
    }

    @Test
    public void testWriteNonStringAndOffset() throws Exception {
//        TopicPartition tp = new TopicPartition("testPartition", 11);
//        IngestClient mockClient = mock(IngestClient.class);
//        String db = "testdb1";
//        String table = "testtable1";
//        String basePath = "somepath";
//        long fileThreshold = 100;
//
//        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);
//
//        List<SinkRecord> records = new ArrayList<SinkRecord>();
//        DummyRecord dummyRecord1 = new DummyRecord(1, "a", (long) 2);
//        DummyRecord dummyRecord2 = new DummyRecord(2, "b", (long) 4);
//
//        records.add(new SinkRecord("topic", 1, null, null, null, dummyRecord1, 10));
//        records.add(new SinkRecord("topic", 2, null, null, null, dummyRecord2, 3));
//        records.add(new SinkRecord("topic", 2, null, null, null, dummyRecord2, 4));
//
//        for (SinkRecord record : records) {
//            writer.writeRecord(record);
//        }
//
//        Assert.assertEquals(writer.getFilePath(), "kafka_testPartition_11_0");
    }

    @Test
    public void testWriteStringyValuesAndOffset() throws Exception {
        TopicPartition tp = new TopicPartition("testTopic", 2);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        long fileThreshold = 100;
        long flushInterval = 300000;
        TopicIngestionProperties props = new TopicIngestionProperties();

        props.ingestionProperties = new IngestionProperties(db, table);
        props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, props, basePath, fileThreshold, flushInterval,config);


        writer.open();
        List<SinkRecord> records = new ArrayList<SinkRecord>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assert.assertEquals(writer.fileWriter.currentFile.path, Paths.get(basePath, String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 3, IngestionProperties.DATA_FORMAT.csv.name())).toString());
        writer.close();
    }

    @Test
    public void testWriteStringValuesAndOffset() throws IOException {
        TopicPartition tp = new TopicPartition("testPartition", 11);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        String[] messages = new String[]{ "stringy message", "another,stringy,message", "{'also':'stringy','sortof':'message'}"};

        // Expect to finish file after writing forth message cause of fileThreshold
        long fileThreshold = messages[0].length() + messages[1].length() + messages[2].length() + messages[2].length() - 1;
        long flushInterval = 300000;
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(db, table);
        props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, props, basePath, fileThreshold, flushInterval, config);

        writer.open();
        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[0], 10));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[1], 13));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 14));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 15));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, messages[2], 16));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assert.assertEquals((long) writer.lastCommittedOffset, (long) 15);
        Assert.assertEquals(writer.currentOffset, 16);

        String currentFileName = writer.fileWriter.currentFile.path;
        Assert.assertEquals(currentFileName, Paths.get(basePath, String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 15, IngestionProperties.DATA_FORMAT.csv.name())).toString());

        // Read
        writer.fileWriter.finishFile(false);
        Function<SourceFile, String> assertFileConsumer = FileWriterTest.getAssertFileConsumer(messages[2] + "\n");
        assertFileConsumer.apply(writer.fileWriter.currentFile);
        writer.close();
    }

    @Test
    public void testWriteBytesValuesAndOffset() throws IOException {
        TopicPartition tp = new TopicPartition("testPartition", 11);
        IngestClient mockClient = mock(IngestClient.class);

        String db = "testdb1";
        String table = "testtable1";
        String basePath = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        FileInputStream fis = new FileInputStream("src/test/resources/data.avro");
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        int content;
        while ((content = fis.read()) != -1) {
            // convert to char and display it
            o.write(content);
        }
        // Expect to finish file with one record although fileThreshold is high
        long fileThreshold = 128;
        long flushInterval = 300000;
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(db, table);
        props.ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.avro);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, props, basePath, fileThreshold, flushInterval, config);

        writer.open();
        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, o.toByteArray(), 10));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assert.assertEquals((long) writer.lastCommittedOffset, (long) 10);
        Assert.assertEquals(writer.currentOffset, 10);

        String currentFileName = writer.fileWriter.currentFile.path;
        Assert.assertEquals(currentFileName, Paths.get(basePath, String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 10, IngestionProperties.DATA_FORMAT.avro.name())).toString());
        writer.close();
    }

  protected Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("kusto.url","xxx");
    props.put("kusto.tables.topics.mapping","[{'topic': 'xxx','db': 'xxx', 'table': 'xxx','format': 'avro', 'mapping':'avri'}]");
    return props;
  }
}
