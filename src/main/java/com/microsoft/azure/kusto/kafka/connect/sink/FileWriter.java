package com.microsoft.azure.kusto.kafka.connect.sink;

import com.google.common.base.Function;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.AvroRecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.ByteRecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.JsonRecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.StringRecordWriterProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.GZIPOutputStream;

/**
 * This class is used to write gzipped rolling files.
 * Currently supports size based rolling, where size is for *uncompressed* size,
 * so final size can vary.
 */
public class FileWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FileWriter.class);

    SourceFile currentFile;
    private Timer timer;
    private Consumer<SourceFile> onRollCallback;
    private final long flushInterval;
    private Function<Long, String> getFilePath;
    private OutputStream outputStream;
    private String basePath;
    private CountingOutputStream countingStream;
    private long fileThreshold;
    // Lock is given from TopicPartitionWriter to lock while ingesting
    private ReentrantReadWriteLock reentrantReadWriteLock;
    // Don't remove! File descriptor is kept so that the file is not deleted when stream is closed
    private FileDescriptor currentFileDescriptor;
    private String flushError;
    private RecordWriterProvider recordWriterProvider;
    private RecordWriter recordWriter;
    private final IngestionProperties.DataFormat format;
    private BehaviorOnError behaviorOnError;
    private boolean shouldWriteAvroAsBytes = false;

    /**
     * @param basePath       - This is path to which to write the files to.
     * @param fileThreshold  - Max size, uncompressed bytes.
     * @param onRollCallback - Callback to allow code to execute when rolling a file. Blocking code.
     * @param getFilePath    - Allow external resolving of file name.
     * @param behaviorOnError - Either log, fail or ignore errors based on the mode.
     */
    public FileWriter(String basePath,
                      long fileThreshold,
                      Consumer<SourceFile> onRollCallback,
                      Function<Long, String> getFilePath,
                      long flushInterval,
                      ReentrantReadWriteLock reentrantLock,
                      IngestionProperties.DataFormat format,
                      BehaviorOnError behaviorOnError) {
        this.getFilePath = getFilePath;
        this.basePath = basePath;
        this.fileThreshold = fileThreshold;
        this.onRollCallback = onRollCallback;
        this.flushInterval = flushInterval;
        this.behaviorOnError = behaviorOnError;

        // This is a fair lock so that we flush close to the time intervals
        this.reentrantReadWriteLock = reentrantLock;

        // If we failed on flush we want to throw the error from the put() flow.
        flushError = null;
        this.format = format;

    }

    boolean isDirty() {
        return this.currentFile != null && this.currentFile.rawBytes > 0;
    }

    public void openFile(@Nullable Long offset) throws IOException {
        SourceFile fileProps = new SourceFile();

        File folder = new File(basePath);
        if (!folder.exists() && !folder.mkdirs()) {
            if (!folder.exists()) {
                throw new IOException(String.format("Failed to create new directory %s", folder.getPath()));
            }
            log.warn("Couldn't create the directory because it already exists (likely a race condition)");
        }

        String filePath = getFilePath.apply(offset);
        fileProps.path = filePath;
        File file = new File(filePath);

        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);
        currentFileDescriptor = fos.getFD();
        fos.getChannel().truncate(0);
        fileProps.file = file;
        currentFile = fileProps;
        countingStream = new CountingOutputStream(new GZIPOutputStream(fos));
        outputStream = countingStream.getOutputStream();
        recordWriter = recordWriterProvider.getRecordWriter(currentFile.path, countingStream);
    }

    void rotate(@Nullable Long offset) throws IOException, DataException {
        finishFile(true);
        openFile(offset);
    }

    void finishFile(Boolean delete) throws IOException, DataException {
        if (isDirty()) {
            recordWriter.commit();
            GZIPOutputStream gzip = (GZIPOutputStream) outputStream;
            gzip.finish();
            try {
                onRollCallback.accept(currentFile);
              } catch (ConnectException e) {
                /*
                 * Swallow the exception and continue to process subsequent records
                 * when behavior.on.error is not set to fail mode.
                 *
                 * Also, throwing/logging the exception with just a message to
                 * avoid polluting logs with duplicate trace.
                 */
                handleErrors("Failed to write records to KustoDB.", e);
            }
            if (delete){
                dumpFile();
            }
        } else {
            outputStream.close();
        }
    }

    private void handleErrors(String message, Exception e) {
        if (KustoSinkConfig.BehaviorOnError.FAIL == behaviorOnError) {
            throw new ConnectException(message, e);
        } else if (KustoSinkConfig.BehaviorOnError.LOG == behaviorOnError) {
            log.error("{}", message, e);
        } else {
            log.debug("{}", message, e);
        }
    }

    private void dumpFile() throws IOException {
        countingStream.close();
        currentFileDescriptor = null;
        boolean deleted = currentFile.file.delete();
        if (!deleted) {
            log.warn("couldn't delete temporary file. File exists: " + currentFile.file.exists());
        }
        currentFile = null;
    }

    public void rollback() throws IOException {
        if (countingStream != null) {
            countingStream.close();
            if (currentFile != null && currentFile.file != null) {
                dumpFile();
            }
        }
    }

    public void close() throws IOException, DataException {
        if (timer != null) {
            timer.cancel();
        }

        // Flush last file, updating index
        finishFile(true);

        // Setting to null so subsequent calls to close won't write it again
        currentFile = null;
    }

    // Set shouldDestroyTimer to true if the current running task should be cancelled
    private void resetFlushTimer(Boolean shouldDestroyTimer) {
        if (flushInterval > 0) {
            if (shouldDestroyTimer) {
                if (timer != null) {
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
            if (timer != null) {
                timer.schedule(t, flushInterval);
            }
        }
    }

    void flushByTimeImpl() {
        // Flush time interval gets the write lock so that it won't starve
        try(AutoCloseableLock ignored = new AutoCloseableLock(reentrantReadWriteLock.writeLock())) {
            // Lock before the check so that if a writing process just flushed this won't ingest empty files
            if (isDirty()) {
                finishFile(true);
            }
            resetFlushTimer(false);
        } catch (Exception e) {
            String fileName = currentFile == null ? "no file created yet" : currentFile.file.getName();
            long currentSize = currentFile == null ? 0 : currentFile.rawBytes;
            flushError = String.format("Error in flushByTime. Current file: %s, size: %d. ", fileName, currentSize);
            log.error(flushError, e);
        }
    }

    public synchronized void writeData(SinkRecord record) throws IOException, DataException {
        if (flushError != null) {
            throw new ConnectException(flushError);
        }
        if (record == null) return;
        if (recordWriterProvider == null) {
            initializeRecordWriter(record);
        }

        if (currentFile == null) {
            openFile(record.kafkaOffset());
            resetFlushTimer(true);
        }
        recordWriter.write(record);
        currentFile.records.add(record);
        currentFile.rawBytes = countingStream.numBytes;
        currentFile.numRecords++;
        if (this.flushInterval == 0 || currentFile.rawBytes > fileThreshold || shouldWriteAvroAsBytes) {
            rotate(record.kafkaOffset());
            resetFlushTimer(true);
        }
    }

    public void initializeRecordWriter(SinkRecord record) {
        if (record.value() instanceof Map) {
            recordWriterProvider = new JsonRecordWriterProvider();
        }
        else if ((record.valueSchema() != null) && (record.valueSchema().type() == Schema.Type.STRUCT)) {
            if (format.equals(IngestionProperties.DataFormat.JSON)) {
                recordWriterProvider = new JsonRecordWriterProvider();
            } else if(format.equals(IngestionProperties.DataFormat.AVRO)) {
                recordWriterProvider = new AvroRecordWriterProvider();
            } else {
                throw new ConnectException(String.format("Invalid Kusto table mapping, Kafka records of type "
                   + "Avro and JSON can only be ingested to Kusto table having Avro or JSON mapping. "
                   + "Currently, it is of type %s.", format));
            }
        }
        else if ((record.valueSchema() == null) || (record.valueSchema().type() == Schema.Type.STRING)){
            recordWriterProvider = new StringRecordWriterProvider();
        }
        else if ((record.valueSchema() != null) && (record.valueSchema().type() == Schema.Type.BYTES)){
            recordWriterProvider = new ByteRecordWriterProvider();
            if(format.equals(IngestionProperties.DataFormat.AVRO)) {
                shouldWriteAvroAsBytes = true;
            }
        } else {
            throw new ConnectException(String.format("Invalid Kafka record format, connector does not support %s format. This connector supports Avro, Json with schema, Json without schema, Byte, String format. ",record.valueSchema().type()));
        }
    }

    private class CountingOutputStream extends FilterOutputStream {
        private long numBytes = 0;
        private OutputStream outputStream;

        CountingOutputStream(OutputStream out) {
            super(out);
            this.outputStream = out;
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

        public OutputStream getOutputStream() {
            return outputStream;
        }
    }
}


