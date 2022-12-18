package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Function;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;

import static com.microsoft.azure.kusto.kafka.connect.sink.Utils.createDirectoryWithPermissions;
import static com.microsoft.azure.kusto.kafka.connect.sink.Utils.getFilesCount;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

public class FileWriterTest {
    IngestionProperties ingestionProps;
    private File currentDirectory;

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
                    String s = out.toString();

                    Assertions.assertEquals(s, msg);
                }
            } catch (IOException e) {
                e.printStackTrace();
                Assertions.fail(e.getMessage());
            }
            return null;
        };
    }

    @BeforeEach
    public final void before() {
        currentDirectory = Utils.getCurrentWorkingDirectory();
        ingestionProps = new IngestionProperties("db", "table");
        ingestionProps.setDataFormat(IngestionProperties.DataFormat.CSV);
    }

    @AfterEach
    public final void afterEach() {
        FileUtils.deleteQuietly(currentDirectory);
    }

    @Test
    public void testOpen() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testWriterOpen").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        Assertions.assertEquals(0, getFilesCount(path));
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
            Assertions.assertEquals(1, getFilesCount(path));
            Assertions.assertEquals(0, fileWriter.currentFile.rawBytes);
            Assertions.assertEquals(FILE_PATH, fileWriter.currentFile.path);
            Assertions.assertTrue(fileWriter.currentFile.file.canWrite());
            fileWriter.rollback();
        }
    }

    @Test
    public void testGzipFileWriter() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        Assertions.assertEquals(0, getFilesCount(path));
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
            Assertions.assertEquals(1, getFilesCount(path));
            // close current file
            fileWriter.rotate(54L);
            Assertions.assertEquals(5, files.size());
            List<Long> sortedFiles = new ArrayList<>(files.values());
            sortedFiles.sort((Long x, Long y) -> (int) (y - x));
            Assertions.assertEquals(sortedFiles,
                    Arrays.asList((long) 108, (long) 108, (long) 108, (long) 108, (long) 54));
            // make sure folder is clear once done - with only the new file
            Assertions.assertEquals(1, getFilesCount(path));
        }
    }

    @Test
    public void testGzipFileWriterFlush() throws IOException, InterruptedException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        HashMap<String, Long> files = new HashMap<>();
        final int MAX_FILE_SIZE = 128 * 2;
        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);
        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, java.util.UUID.randomUUID().toString()) + "csv.gz";
        // Expect no files to be ingested as size is small and flushInterval is big
        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true);
        String msg = "Message";
        SinkRecord record = new SinkRecord("topic", 1, null, null, null, msg, 10);
        fileWriter.writeData(record);
        Thread.sleep(1000);
        Assertions.assertEquals(0, files.size());
        fileWriter.rotate(10L);
        fileWriter.stop();
        Assertions.assertEquals(1, files.size());

        String path2 = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2_2").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path2));
        Function<Long, String> generateFileName2 = (Long l) -> Paths.get(path2, java.util.UUID.randomUUID().toString()).toString();
        // Expect one file to be ingested as flushInterval had changed and is shorter than sleep time
        FileWriter fileWriter2 = new FileWriter(path2, MAX_FILE_SIZE, trackFiles, generateFileName2, 1000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true);
        String msg2 = "Second Message";
        SinkRecord record1 = new SinkRecord("topic", 1, null, null, null, msg2, 10);
        fileWriter2.writeData(record1);
        Thread.sleep(1050);
        Assertions.assertEquals(2, files.size());
        List<Long> sortedFiles = new ArrayList<>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assertions.assertEquals(sortedFiles, Arrays.asList((long) 15, (long) 8));
        // make sure folder is clear once done
        fileWriter2.close();
        Assertions.assertEquals(1, getFilesCount(path));
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
            // return null;
        };
        String path = Paths.get(currentDirectory.getPath(), "offsetCheckByInterval").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        Function<Long, String> generateFileName = (Long offset) -> {
            if (offset == null) {
                offset = offsets.currentOffset;
            }
            return Paths.get(path, Long.toString(offset)).toString();
        };
        try (FileWriter fileWriter2 = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 500, reentrantReadWriteLock,
                ingestionProps.getDataFormat(),
                BehaviorOnError.FAIL, true)) {
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
            Thread.sleep(550);
            // Assertions
            Assertions.assertEquals(2, files.size());

            // Make sure that the first file is from offset 1 till 2 and second is from 3 till 3
            Assertions.assertEquals(30L, files.stream().map(Map.Entry::getValue).toArray(Long[]::new)[0]);
            Assertions.assertEquals(15L, files.stream().map(Map.Entry::getValue).toArray(Long[]::new)[1]);
            Assertions.assertEquals("1",
                    files.stream().map((s) -> s.getKey().substring(path.length() + 1)).toArray(String[]::new)[0]);
            Assertions.assertEquals("3",
                    files.stream().map((s) -> s.getKey().substring(path.length() + 1)).toArray(String[]::new)[1]);
            Assertions.assertEquals(committedOffsets, new ArrayList<Long>() {
                {
                    add(2L);
                    add(3L);
                }
            });
            // make sure folder is clear once done
            fileWriter2.stop();
            Assertions.assertEquals(0, getFilesCount(path));
        }
    }
}
