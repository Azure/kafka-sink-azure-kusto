package com.microsoft.azure.kusto.kafka.connect.sink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

public class FileWriterTest {
    private static IngestionProperties ingestionProps;
    private static File currentDirectory;

    static Function<SourceFile, String> getAssertFileConsumerFunction(String msg) {
        return (SourceFile f) -> {
            try (FileInputStream fileInputStream = new FileInputStream(f.file)) {
                byte[] bytes = IOUtils.toByteArray(fileInputStream);
                try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
                        GZIPInputStream gzipper = new GZIPInputStream(bin);
                        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = gzipper.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }
                    String s = out.toString();
                    Assertions.assertEquals(s, msg);
                }
            } catch (IOException e) {
                Assertions.fail(e.getMessage());
            }
            return null;
        };
    }

    @BeforeAll
    public static void beforeAll() {
        currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                FileWriter.class.getSimpleName(),
                String.valueOf(Instant.now().toEpochMilli())).toString());
        ingestionProps = new IngestionProperties("db", "table");
        ingestionProps.setDataFormat(IngestionProperties.DataFormat.CSV);
    }

    @AfterAll
    public static void afterAll() {
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
        Assertions.assertTrue(mkdirs);
        Assertions.assertEquals(0, Objects.requireNonNull(folder.listFiles()).length);
        final String FILE_PATH = Paths.get(path, "ABC").toString();
        final int MAX_FILE_SIZE = 128;
        Consumer<SourceFile> trackFiles = (SourceFile f) -> {
        };
        Function<Long, String> generateFileName = (Long l) -> FILE_PATH;
        try (FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true)) {
            String msg = "Line number 1: This is a message from the other size";
            SinkRecord record = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg.getBytes(), 10);
            fileWriter.initializeRecordWriter(record);
            fileWriter.openFile(null);
            Assertions.assertEquals(1, Objects.requireNonNull(folder.listFiles()).length);
            Assertions.assertEquals(0, fileWriter.currentFile.rawBytes);
            Assertions.assertEquals(FILE_PATH, fileWriter.currentFile.path);
            Assertions.assertTrue(fileWriter.currentFile.file.canWrite());
            fileWriter.rollback();
        }
    }

    @Test
    public void testGzipFileWriter() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter").toString();
        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assertions.assertTrue(mkdirs);
        Assertions.assertEquals(0, Objects.requireNonNull(folder.listFiles()).length);
        HashMap<String, Long> files = new HashMap<>();
        final int MAX_FILE_SIZE = 100;
        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);
        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, String.valueOf(java.util.UUID.randomUUID())) + "csv.gz";
        try (FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true)) {
            for (int i = 0; i < 9; i++) {
                String msg = String.format("Line number %d : This is a message from the other size", i);
                SinkRecord record1 = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg.getBytes(), 10);
                fileWriter.writeData(record1);
            }
            Assertions.assertEquals(4, files.size());
            // should still have 1 open file at this point...
            Assertions.assertEquals(1, Objects.requireNonNull(folder.listFiles()).length);
            fileWriter.rotate(54L);
            Assertions.assertEquals(5, files.size());
            List<Long> sortedFiles = new ArrayList<>(files.values());
            sortedFiles.sort((Long x, Long y) -> (int) (y - x));
            Assertions.assertEquals(sortedFiles, Arrays.asList((long) 108, (long) 108, (long) 108, (long) 108, (long) 54));
            // make sure folder is clear once done - with only the new file
            Assertions.assertEquals(1, Objects.requireNonNull(folder.listFiles()).length);
        }
    }

    @Test
    public void testGzipFileWriterFlush() throws IOException, InterruptedException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2").toString();
        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assertions.assertTrue(mkdirs);
        HashMap<String, Long> files = new HashMap<>();
        final int MAX_FILE_SIZE = 128 * 2;
        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);
        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, java.util.UUID.randomUUID().toString()) + "csv.gz";
        String path2 = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2_2").toString();
        File folder2 = new File(path2);
        Function<Long, String> generateFileName2 = (Long l) -> Paths.get(path2, java.util.UUID.randomUUID().toString()).toString();
        // Expect no files to be ingested as size is small and flushInterval is big
        try (FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true);
                FileWriter fileWriter2 = new FileWriter(path2, MAX_FILE_SIZE, trackFiles, generateFileName2, 1000, new ReentrantReadWriteLock(),
                        ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true)) {
            String msg = "Message";
            SinkRecord record = new SinkRecord("topic", 1, null, null, null, msg, 10);
            fileWriter.writeData(record);
            Thread.sleep(1000);
            Assertions.assertEquals(0, files.size());
            fileWriter.rotate(10L);
            fileWriter.stop();
            Assertions.assertEquals(1, files.size());
            mkdirs = folder2.mkdirs();
            Assertions.assertTrue(mkdirs);
            String msg2 = "Second Message";
            SinkRecord record1 = new SinkRecord("topic", 1, null, null, null, msg2, 10);
            fileWriter2.writeData(record1);
            Thread.sleep(1050);
            Assertions.assertEquals(2, files.size());
            List<Long> sortedFiles = new ArrayList<>(files.values());
            sortedFiles.sort((Long x, Long y) -> (int) (y - x));
            Assertions.assertEquals(sortedFiles, Arrays.asList((long) 15, (long) 8));
            Assertions.assertEquals(1, Objects.requireNonNull(folder.listFiles()).length);
        }
    }

    @Test
    public void offsetCheckByInterval() throws InterruptedException, IOException {
        // This test will check that lastCommitOffset is set to the right value, when ingests are done by flush interval.
        // There will be a write operation followed by a flush which will track files and sleep.
        // While it sleeps there will be another write attempt which should wait on the lock and another flush later.
        // Resulting in first record to be with offset 1 and second with offset 2.
        AtomicLong currentOffset = new AtomicLong(0);
        ArrayList<Map.Entry<String, Long>> files = new ArrayList<>();
        final int MAX_FILE_SIZE = 128 * 2;
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        final ArrayList<Long> committedOffsets = new ArrayList<>();
        Consumer<SourceFile> trackFiles = (SourceFile f) -> {
            committedOffsets.add(currentOffset.get());
            files.add(new AbstractMap.SimpleEntry<>(f.path, f.rawBytes));
            // return null;
        };
        String path = Paths.get(currentDirectory.getPath(), "offsetCheckByInterval").toString();
        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assertions.assertTrue(mkdirs);
        Function<Long, String> generateFileName = (Long offset) -> {
            if (offset == null) {
                offset = currentOffset.get();
            }
            return Paths.get(path, Long.toString(offset)).toString();
        };
        try (FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 500, reentrantReadWriteLock,
                ingestionProps.getDataFormat(),
                BehaviorOnError.FAIL, true)) {
            String msg2 = "Second Message";
            reentrantReadWriteLock.readLock().lock();
            long recordOffset = 1;
            SinkRecord record = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);
            fileWriter.writeData(record);
            currentOffset.set(recordOffset);
            // Wake the flush by interval in the middle of the writing
            Thread.sleep(510);
            recordOffset = 2;
            SinkRecord record2 = new SinkRecord("TestTopic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);
            fileWriter.writeData(record2);
            currentOffset.set(recordOffset);
            reentrantReadWriteLock.readLock().unlock();
            // Context switch
            Thread.sleep(10);
            reentrantReadWriteLock.readLock().lock();
            recordOffset = 3;
            SinkRecord record3 = new SinkRecord("TestTopic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);
            currentOffset.set(recordOffset);
            fileWriter.writeData(record3);
            reentrantReadWriteLock.readLock().unlock();
            Thread.sleep(550);
            // Assertions
            Assertions.assertEquals(2, files.size());
            // Make sure that the first file is from offset 1 till 2 and second is from 3 till 3
            Assertions.assertEquals(30L, files.stream().map(Map.Entry::getValue).toArray(Long[]::new)[0]);
            Assertions.assertEquals(15L, files.stream().map(Map.Entry::getValue).toArray(Long[]::new)[1]);
            Assertions.assertEquals("1", files.stream().map((s) -> s.getKey().substring(path.length() + 1)).toArray(String[]::new)[0]);
            Assertions.assertEquals("3", files.stream().map((s) -> s.getKey().substring(path.length() + 1)).toArray(String[]::new)[1]);
            Assertions.assertEquals(committedOffsets, new ArrayList<Long>() {
                {
                    add(2L);
                    add(3L);
                }
            });
            // make sure folder is clear once done
            fileWriter.stop();
            Assertions.assertEquals(0, Objects.requireNonNull(folder.listFiles()).length);
        }
    }
}
