package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class GZIPFileWriterTest {
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
    public void testOpen() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testWriterOpen").toString();

        File folder = new File(path);
        folder.mkdirs();

        Assert.assertEquals(folder.listFiles().length, 0);

        HashMap<String, Long> files = new HashMap<String, Long>();

        final String FILE_PATH = Paths.get(path, "ABC").toString();
        final int MAX_FILE_SIZE = 128;

        Consumer<GZIPFileDescriptor> trackFiles = (GZIPFileDescriptor f) -> {
            files.put(f.path, f.rawBytes);
        };

        Supplier<String> generateFileName = () -> FILE_PATH;

        GZIPFileWriter gzipFileWriter = new GZIPFileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName);

        gzipFileWriter.openFile();

        Assert.assertEquals(folder.listFiles().length, 1);
        Assert.assertEquals(gzipFileWriter.currentFile.rawBytes, 0);
        Assert.assertEquals(gzipFileWriter.currentFile.path, FILE_PATH + ".gz");
        Assert.assertTrue(gzipFileWriter.currentFile.file.canWrite());

        gzipFileWriter.rollback();
    }

    @Test
    public void testGzipFileWriter() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter").toString();

        File folder = new File(path);
        folder.mkdirs();

        Assert.assertEquals(folder.listFiles().length, 0);

        HashMap<String, Long> files = new HashMap<String, Long>();

        final int MAX_FILE_SIZE = 128;

        Consumer<GZIPFileDescriptor> trackFiles = (GZIPFileDescriptor f) -> {
            files.put(f.path, f.rawBytes);
        };


        Supplier<String> generateFileName = () -> Paths.get(path, String.valueOf(java.util.UUID.randomUUID())).toString();

        GZIPFileWriter gzipFileWriter = new GZIPFileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName);

        for (int i = 0; i < 9; i++) {
            String msg = String.format("Line number %d : This is a message from the other size", i);
            gzipFileWriter.write(msg.getBytes("UTF-8"));
        }

        Assert.assertEquals(files.size(), 4);

        // should still have 1 open file at this point...
        Assert.assertEquals(folder.listFiles().length, 1);

        // close current file
        gzipFileWriter.close();
        Assert.assertEquals(files.size(), 5);

        List<Long> sortedFiles = new ArrayList<Long>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assert.assertEquals(sortedFiles, Arrays.asList((long) 106, (long) 106, (long) 106, (long) 106, (long) 53));

        // make sure folder is clear once done
        Assert.assertEquals(folder.listFiles().length, 0);
    }
}
