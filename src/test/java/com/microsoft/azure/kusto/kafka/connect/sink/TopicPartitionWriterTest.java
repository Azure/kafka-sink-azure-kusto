package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class TopicPartitionWriterTest {
    // TODO: should probably find a better way to mock internal class (GZIPFileWriter)...
    File currentDirectory;

    @Before
    public final void before() {
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                GZIPFileWriter.class.getSimpleName(),
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
    public void testHandleRollfile() {
        TopicPartition tp = new TopicPartition("testPartition", 11);
        IngestClient mockedClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, db, table, basePath, fileThreshold);

        GZIPFileDescriptor descriptor = new GZIPFileDescriptor();
        descriptor.rawBytes = 1024;
        descriptor.path = "somepath/somefile";

        writer.handleRollFile(descriptor);

        FileSourceInfo fileSourceInfo = new FileSourceInfo(descriptor.path, descriptor.rawBytes);
        IngestionProperties kustoIngestionProperties = new IngestionProperties(db, table);
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

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);

        Assert.assertEquals(writer.getFilePath(), Paths.get(basePath, "kafka_testTopic_11_0").toString());
    }

    @Test
    public void testGetFilenameAfterOffsetChanges() {
        TopicPartition tp = new TopicPartition("testTopic", 11);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);
        writer.open();
        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assert.assertEquals(writer.getFilePath(), Paths.get(basePath, "kafka_testTopic_11_5").toString());
    }

    @Test
    public void testOpenClose() {
        TopicPartition tp = new TopicPartition("testPartition", 1);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = "somepath";
        long fileThreshold = 100;

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);
        writer.open();
        writer.close();
    }

    @Test
    public void testWriteNonStringAndOffset() throws Exception {
//        TopicPartition tp = new TopicPartition("testPartition", 11);
//        KustoIngestClient mockClient = mock(KustoIngestClient.class);
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

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);


        writer.open();
        List<SinkRecord> records = new ArrayList<SinkRecord>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        Assert.assertEquals(writer.gzipFileWriter.currentFile.path, Paths.get(basePath, String.format("kafka_%s_%d_%d.gz", tp.topic(), tp.partition(), 0)).toString());
    }

    @Test
    public void testWriteBytesValuesAndOffset() throws Exception {
        TopicPartition tp = new TopicPartition("testPartition", 11);
        IngestClient mockClient = mock(IngestClient.class);
        String db = "testdb1";
        String table = "testtable1";
        String basePath = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        long fileThreshold = 50;

        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, db, table, basePath, fileThreshold);

        writer.open();
        List<SinkRecord> records = new ArrayList<SinkRecord>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), 10));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "another,stringy,message".getBytes(StandardCharsets.UTF_8), 13));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "{'also':'stringy','sortof':'message'}".getBytes(StandardCharsets.UTF_8), 14));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "{'also':'stringy','sortof':'message'}".getBytes(StandardCharsets.UTF_8), 15));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "{'also':'stringy','sortof':'message'}".getBytes(StandardCharsets.UTF_8), 16));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "{'also':'stringy','sortof':'message'}".getBytes(StandardCharsets.UTF_8), 17));

        for (SinkRecord record : records) {
            writer.writeRecord(record);
        }

        //TODO : file threshold ignored?
        Assert.assertTrue(writer.lastCommittedOffset.equals((long) 15));
        Assert.assertEquals(writer.currentOffset, 17);
        Assert.assertEquals(writer.gzipFileWriter.currentFile.path, Paths.get(basePath, String.format("kafka_%s_%d_%d.gz", tp.topic(), tp.partition(), 16)).toString());
    }
}
