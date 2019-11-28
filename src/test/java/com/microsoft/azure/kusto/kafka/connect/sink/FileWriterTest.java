package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class FileWriterTest {
    private File currentDirectory;

    @Before
    public final void before() {
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
    public void testOpen() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testWriterOpen").toString();

        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);

        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);

        final String FILE_PATH = Paths.get(path, "ABC").toString();
        final int MAX_FILE_SIZE = 128;

        Consumer<FileDescriptor> trackFiles = (FileDescriptor f) -> {};

        Supplier<String> generateFileName = () -> FILE_PATH;

        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, null);

        fileWriter.openFile();

        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 1);
        Assert.assertEquals(fileWriter.currentFile.rawBytes, 0);
        Assert.assertEquals(fileWriter.currentFile.path, FILE_PATH + ".gz");
        Assert.assertTrue(fileWriter.currentFile.file.canWrite());

        fileWriter.rollback();
    }

    @Test
    public void testGzipFileWriter() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter").toString();

        File folder = new File(path);
        boolean mkdirs = folder.mkdirs();
        Assert.assertTrue(mkdirs);

        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);

        HashMap<String, Long> files = new HashMap<>();

        final int MAX_FILE_SIZE = 128;

        Consumer<FileDescriptor> trackFiles = (FileDescriptor f) -> files.put(f.path, f.rawBytes);

        Supplier<String> generateFileName = () -> Paths.get(path, String.valueOf(java.util.UUID.randomUUID())).toString();

        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, null);

        for (int i = 0; i < 9; i++) {
            String msg = String.format("Line number %d : This is a message from the other size", i);
            fileWriter.write(msg.getBytes(StandardCharsets.UTF_8));
        }

        Assert.assertEquals(files.size(), 4);

        // should still have 1 open file at this point...
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 1);

        // close current file
        fileWriter.close();
        Assert.assertEquals(files.size(), 5);

        List<Long> sortedFiles = new ArrayList<>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assert.assertEquals(sortedFiles, Arrays.asList((long) 106, (long) 106, (long) 106, (long) 106, (long) 53));

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

        Consumer<FileDescriptor> trackFiles = (FileDescriptor f) -> files.put(f.path, f.rawBytes);

        Supplier<String> generateFileName = () -> Paths.get(path, java.util.UUID.randomUUID().toString()).toString();

        // Expect no files to be ingested as size is small and flushInterval is big
        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, null);

        String msg = "Message";
        fileWriter.write(msg.getBytes(StandardCharsets.UTF_8));
        Thread.sleep(1000);

        Assert.assertEquals(files.size(), 0);
        fileWriter.close();
        Assert.assertEquals(files.size(), 1);

        String path2 = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2_2").toString();
        File folder2 = new File(path2);
        mkdirs = folder2.mkdirs();
        Assert.assertTrue(mkdirs);

        Supplier<String> generateFileName2 = () -> Paths.get(path2, java.util.UUID.randomUUID().toString()).toString();
        // Expect one file to be ingested as flushInterval had changed
        FileWriter fileWriter2 = new FileWriter(path2, MAX_FILE_SIZE, trackFiles, generateFileName2, 1000, null);

        String msg2 = "Second Message";

        fileWriter2.write(msg2.getBytes(StandardCharsets.UTF_8));
        Thread.sleep(1010);

        Assert.assertEquals(files.size(), 2);


        List<Long> sortedFiles = new ArrayList<>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assert.assertEquals(sortedFiles, Arrays.asList((long) 14, (long) 7));

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

        Consumer<FileDescriptor> trackFiles = (FileDescriptor f) -> {
            try (FileInputStream fileInputStream = new FileInputStream(f.file)){
                byte[] bytes = IOUtils.toByteArray(fileInputStream);
                try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
                     GZIPInputStream gzipper = new GZIPInputStream(bin)){

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

        Supplier<String> generateFileName = () -> Paths.get(path, java.util.UUID.randomUUID().toString()).toString() + ".csv";

        // Expect no files to be ingested as size is small and flushInterval is big
        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 0, CompressionType.gz);

        gzipOutputStream.write(msg.getBytes());
        gzipOutputStream.finish();
        fileWriter.write(byteArrayOutputStream.toByteArray());

        fileWriter.close();
        Assert.assertEquals(Objects.requireNonNull(folder.listFiles()).length, 0);


    }
}
