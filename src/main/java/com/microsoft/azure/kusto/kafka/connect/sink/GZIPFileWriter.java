package com.microsoft.azure.kusto.kafka.connect.sink;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;


/**
 * This class is used to write gzipped rolling files.
 * Currently supports size based rolling, where size is for *uncompressed* size,
 * so final size can vary.
 */
public class GZIPFileWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private Timer timer;
    public GZIPFileDescriptor currentFile;
    private Consumer<GZIPFileDescriptor> onRollCallback;
    private long flushInterval;
    private Supplier<String> getFilePath;
    private GZIPOutputStream gzipStream;
    private String basePath;
    private CountingOutputStream fileStream;
    private long fileThreshold;

    /**
     * @param basePath       - This is path to which to write the files to.
     * @param fileThreshold  - Max size, uncompressed bytes.
     * @param onRollCallback - Callback to allow code to execute when rolling a file. Blocking code.
     * @param getFilePath    - Allow external resolving of file name.
     */
    public GZIPFileWriter(String basePath,
                          long fileThreshold,
                          Consumer<GZIPFileDescriptor> onRollCallback,
                          Supplier<String> getFilePath,
                          long flushInterval) {
        this.getFilePath = getFilePath;
        this.basePath = basePath;
        this.fileThreshold = fileThreshold;
        this.onRollCallback = onRollCallback;
        this.flushInterval = flushInterval;
    }

    // Set shouldDestroyTimer to true if the current running task should be cancelled
    private void resetFlushTimer(Boolean shouldDestroyTimer) {
        if(shouldDestroyTimer) {
            if(timer != null){
                timer.purge();
                timer.cancel();
            }

            timer = new Timer(true);
        }
        
        TimerTask t = new TimerTask() {
            @Override
            public void run() {
                flushByTimeImpl();
            }
        };
        timer.schedule(t, flushInterval);
    }

    private void flushByTimeImpl() {
        try {
            if(currentFile != null && currentFile.rawBytes > 0){
                rotate();
            }
            resetFlushTimer(false);
        } catch (Exception e) {
            String fileName = currentFile == null ? "no file created yet" :   currentFile.file.getName();
            long currentSize = currentFile == null ? 0 :   currentFile.rawBytes;
            log.error(String.format("Error in flushByTime. Current file: %s, size: %d. ", fileName, currentSize), e);
        }
    }

    public boolean isDirty() {
        return this.currentFile != null && this.currentFile.rawBytes > 0;
    }

    public synchronized void write(byte[] data) throws IOException {

        if (data == null || data.length == 0) return;

        if (currentFile == null) {
            openFile();
            resetFlushTimer(false);
        }

        if ((currentFile.rawBytes + data.length) > fileThreshold) {
            rotate();
            resetFlushTimer(true);
        }

        gzipStream.write(data);

        currentFile.rawBytes += data.length;
        currentFile.zippedBytes += fileStream.numBytes;
        currentFile.numRecords++;
    }

    public void openFile() throws IOException {
        GZIPFileDescriptor fileDescriptor = new GZIPFileDescriptor();

        File folder = new File(basePath);
        if (!folder.exists() && !folder.mkdirs()) {
            throw new IOException(String.format("Failed to create new directory %s", folder.getPath()));
        }

        String filePath = getFilePath.get() + ".gz";
        fileDescriptor.path = filePath;

        File file = new File(filePath);

        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().truncate(0);

        fileStream = new CountingOutputStream(fos);
        gzipStream = new GZIPOutputStream(fileStream);

        fileDescriptor.file = file;

        currentFile = fileDescriptor;
    }

    void rotate() throws IOException {
        finishFile();
        openFile();
    }

    private void finishFile() throws IOException {
        gzipStream.finish();

        onRollCallback.accept(currentFile);
        // closing late so that the success callback will have a chance to use the file.
        gzipStream.close();
        currentFile.file.delete();
    }

    public void rollback() throws IOException {
        if (gzipStream != null) {
            gzipStream.close();
            if (currentFile != null && currentFile.file != null) {
                currentFile.file.delete();
            }
        }
    }

    public void close() throws IOException {
        // Flush last file, updating index
        timer.cancel();
        timer.purge();
        finishFile();
        gzipStream.close();
    }


    private class CountingOutputStream extends FilterOutputStream {
        private long numBytes = 0;

        CountingOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            this.numBytes++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            this.numBytes += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            this.numBytes += len;
        }
    }
}

