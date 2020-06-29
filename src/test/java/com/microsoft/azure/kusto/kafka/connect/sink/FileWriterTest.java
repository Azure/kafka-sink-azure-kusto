package com.microsoft.azure.kusto.kafka.connect.sink;

import com.google.common.base.Function;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap;



import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class FileWriterTest {
    private File currentDirectory;
    private KustoSinkConfig config;
    IngestionProperties ingestionProps;

    @Before
    public final void before() {
        config = new KustoSinkConfig(getProperties());
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                FileWriter.class.getSimpleName(),
                String.valueOf(Instant.now().toEpochMilli())
        ).toString());
        ingestionProps = new IngestionProperties("db", "table");
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
    public void testOpen() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testWriterOpen").toString();
        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);

        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);

        final String FILE_PATH = Paths.get(path, "ABC").toString();
        final int MAX_FILE_SIZE = 128;

        Consumer<SourceFile> trackFiles = (SourceFile f) -> {};

        Function<Long, String> generateFileName = (Long l) -> FILE_PATH;

        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, false, new ReentrantReadWriteLock(), ingestionProps, BehaviorOnError.FAIL);
        String msg = "Line number 1: This is a message from the other size";
        SinkRecord record = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg.getBytes(), 10);
        fileWriter.initializeRecordWriter(record);
        fileWriter.openFile(null);
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 1);
        Assert.assertEquals(fileWriter.currentFile.rawBytes, 0);
        Assert.assertEquals(fileWriter.currentFile.path, FILE_PATH);
        Assert.assertTrue(fileWriter.currentFile.file.canWrite());

        fileWriter.rollback();
    }

    @Test
    public void testGzipFileWriter() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter").toString();
        SinkRecord record = new SinkRecord("TestTopic", 1, null, null, null, "random message", 1);
        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);

        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);

        HashMap<String, Long> files = new HashMap<>();

        final int MAX_FILE_SIZE = 100;

        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);

        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, String.valueOf(java.util.UUID.randomUUID())).toString() + "csv.gz";

        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, false, new ReentrantReadWriteLock(), ingestionProps, BehaviorOnError.FAIL);

        for (int i = 0; i < 9; i++) {
            String msg = String.format("Line number %d : This is a message from the other size", i);
            SinkRecord record1 = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg.getBytes(), 10);
            fileWriter.writeData(record1);
        }

        Assert.assertEquals(files.size(), 4);

        // should still have 1 open file at this point...
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 1);

        // close current file
        fileWriter.close();
        Assert.assertEquals(files.size(), 5);

        List<Long> sortedFiles = new ArrayList<>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assert.assertEquals(sortedFiles, Arrays.asList((long) 108, (long) 108, (long) 108, (long) 108, (long) 54));

        // make sure folder is clear once done
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);
    }

    @Test
    public void testGzipFileWriterFlush() throws IOException, InterruptedException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2").toString();

        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);
        HashMap<String, Long> files = new HashMap<>();

        final int MAX_FILE_SIZE = 128 * 2;

        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);

        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, java.util.UUID.randomUUID().toString()).toString() + "csv.gz";

        // Expect no files to be ingested as size is small and flushInterval is big
        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, false, new ReentrantReadWriteLock(), ingestionProps, BehaviorOnError.FAIL);

        String msg = "Message";
        SinkRecord record = new SinkRecord("topic", 1, null, null, null, msg, 10);
        fileWriter.writeData(record);

        Thread.sleep(1000);

        Assert.assertEquals(files.size(), 0);
        fileWriter.close();
        Assert.assertEquals(files.size(), 1);

        String path2 = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2_2").toString();
        File folder2 = new File(path2);
        mkdirs = folder2.mkdirs();
        Assert.assertTrue(mkdirs);

        Function<Long, String> generateFileName2 = (Long l) -> Paths.get(path2, java.util.UUID.randomUUID().toString()).toString();
        // Expect one file to be ingested as flushInterval had changed
        FileWriter fileWriter2 = new FileWriter(path2, MAX_FILE_SIZE, trackFiles, generateFileName2, 1000, false, new ReentrantReadWriteLock(), ingestionProps, BehaviorOnError.FAIL);

        String msg2 = "Second Message";
        SinkRecord record1 = new SinkRecord("topic", 1, null, null, null, msg2, 10);
        fileWriter2.writeData(record1);
        Thread.sleep(1010);

        Assert.assertEquals(files.size(), 2);

        List<Long> sortedFiles = new ArrayList<>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assert.assertEquals(sortedFiles, Arrays.asList((long) 15, (long) 8));

        // make sure folder is clear once done
        fileWriter2.close();
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);
    }

    @Test
    public void offsetCheckByInterval() throws InterruptedException, IOException {
        // This test will check that lastCommitOffset is set to the right value, when ingests are done by flush interval.
        // There will be a write operation followed by a flush which will track files and sleep.
        // While it sleeps there will be another write attempt which should wait on the lock and another flush later.
        // Resulting in first record to be with offset 1 and second with offset 2.

        ArrayList<Map.Entry<String, Long>> files = new ArrayList<>();
        final int MAX_FILE_SIZE = 128 * 2;
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        final ArrayList<Long> committedOffsets = new ArrayList<>();
        class Offsets {
            private long currentOffset = 0;
        }
        final Offsets offsets = new Offsets();
        Consumer<SourceFile> trackFiles = (SourceFile f) -> {
            committedOffsets.add(offsets.currentOffset);
            files.add(new AbstractMap.SimpleEntry<>(f.path, f.rawBytes));
            //return null;
        };

        String path = Paths.get(currentDirectory.getPath(), "offsetCheckByInterval").toString();
        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);
        Function<Long, String> generateFileName = (Long offset) -> {
            if(offset == null){
                offset = offsets.currentOffset;
            }
            return Paths.get(path, Long.toString(offset)).toString();
        };
        FileWriter fileWriter2 = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 500, false, reentrantReadWriteLock, ingestionProps, BehaviorOnError.FAIL);
        String msg2 = "Second Message";
        reentrantReadWriteLock.readLock().lock();
        long recordOffset = 1;
        SinkRecord record = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);
        fileWriter2.writeData(record);
        offsets.currentOffset = recordOffset;

        // Wake the flush by interval in the middle of the writing
        Thread.sleep(510);
        recordOffset = 2;
        SinkRecord record2 = new SinkRecord("TestTopic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);

        fileWriter2.writeData(record2);
        offsets.currentOffset = recordOffset;
        reentrantReadWriteLock.readLock().unlock();

        // Context switch
        Thread.sleep(10);
        reentrantReadWriteLock.readLock().lock();
        recordOffset = 3;
        SinkRecord record3 = new SinkRecord("TestTopic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);

        offsets.currentOffset = recordOffset;
        fileWriter2.writeData(record3);
        reentrantReadWriteLock.readLock().unlock();

        Thread.sleep(510);

        // Assertions
        Assert.assertEquals(files.size(), 2);

        // Make sure that the first file is from offset 1 till 2 and second is from 3 till 3
        Assert.assertEquals(files.stream().map(Map.Entry::getValue).toArray(Long[]::new), new Long[]{30L, 15L});
        Assert.assertEquals(files.stream().map((s)->s.getKey().substring(path.length() + 1)).toArray(String[]::new), new String[]{"1", "3"});
        Assert.assertEquals(committedOffsets, new ArrayList<Long>(){{add(2L);add(3L);}});

        // make sure folder is clear once done
        fileWriter2.close();
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);
    }

    @Test
    public void testFileWriterCompressed() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2").toString();

        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);
        HashMap<String, Long> files = new HashMap<>();

        final int MAX_FILE_SIZE = 128 * 2;

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        String msg = "Message";

        Consumer<SourceFile> trackFiles = getAssertFileConsumer(msg);

        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, java.util.UUID.randomUUID().toString()).toString() + ".csv.gz";

        // Expect no files to be ingested as size is small and flushInterval is big
        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 0, false, new ReentrantReadWriteLock(), ingestionProps, BehaviorOnError.FAIL);

        gzipOutputStream.write(msg.getBytes());
        gzipOutputStream.finish();
        SinkRecord record = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, byteArrayOutputStream.toByteArray(), 10);
        fileWriter.writeData(record);

        fileWriter.close();
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 1);
    }

    static Function<SourceFile, String> getAssertFileConsumerFunction(String msg) {
        return (SourceFile f) -> {
            try (FileInputStream fileInputStream = new FileInputStream(f.file)) {
                byte[] bytes = IOUtils.toByteArray(fileInputStream);
                try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
                     GZIPInputStream gzipper = new GZIPInputStream(bin)) {

                    byte[] buffer = new byte[1024];
                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                    int len;
                    while ((len = gzipper.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }

                    gzipper.close();
                    out.close();
                    String s = new String(out.toByteArray());

                    Assert.assertEquals(s, msg);
                }
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
            return null;
        };
    }

    protected Map<String, String> getProperties() {
      Map<String, String> settings = new HashMap<>();
      settings.put(KustoSinkConfig.KUSTO_URL_CONF, "xxx");
      settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
      settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
      settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
      settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
      return settings;
    }
    static Consumer<SourceFile> getAssertFileConsumer(String msg) {
        return (SourceFile f) -> {
            try (FileInputStream fileInputStream = new FileInputStream(f.file)) {
                byte[] bytes = IOUtils.toByteArray(fileInputStream);
                try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
                     GZIPInputStream gzipper = new GZIPInputStream(bin)) {

                    byte[] buffer = new byte[1024];
                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                    int len;
                    while ((len = gzipper.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }

                    gzipper.close();
                    out.close();
                    String s = new String(out.toByteArray());

                    Assert.assertEquals(s, msg);
                }
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        };
    }
}
