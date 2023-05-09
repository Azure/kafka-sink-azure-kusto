package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.*;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.AvroRecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.ByteRecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.JsonRecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.StringRecordWriterProvider;

/**
 * This class is used to write gzipped rolling files.
 * Currently supports size based rolling, where size is for *uncompressed* size,
 * so final size can vary.
 */
public class FileWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FileWriter.class);
    private final long flushInterval;
    private final IngestionProperties.DataFormat format;
    SourceFile currentFile;
    private Timer timer;
    private final Consumer<SourceFile> onRollCallback;
    private final Function<Long, String> getFilePath;
    private GZIPOutputStream outputStream;
    private final String basePath;
    private CountingOutputStream countingStream;
    private final long fileThreshold;
    // Lock is given from TopicPartitionWriter to lock while ingesting
    private final ReentrantReadWriteLock reentrantReadWriteLock;
    // Don't remove! File descriptor is kept so that the file is not deleted when stream is closed
    private FileDescriptor currentFileDescriptor;
    private String flushError;
    private RecordWriterProvider recordWriterProvider;
    private RecordWriter recordWriter;
    private final BehaviorOnError behaviorOnError;
    private boolean shouldWriteAvroAsBytes = false;
    private boolean stopped = false;
    private boolean isDlqEnabled = false;

    /**
     * @param basePath        - This is path to which to write the files to.
     * @param fileThreshold   - Max size, uncompressed bytes.
     * @param onRollCallback  - Callback to allow code to execute when rolling a file. Blocking code.
     * @param getFilePath     - Allow external resolving of file name.
     * @param behaviorOnError - Either log, fail or ignore errors based on the mode.
     */
    public FileWriter(String basePath,
            long fileThreshold,
            Consumer<SourceFile> onRollCallback,
            Function<Long, String> getFilePath,
            long flushInterval,
            ReentrantReadWriteLock reentrantLock,
            IngestionProperties.DataFormat format,
            BehaviorOnError behaviorOnError,
            boolean isDlqEnabled) {
        this.getFilePath = getFilePath;
        this.basePath = basePath;
        this.fileThreshold = fileThreshold;
        this.onRollCallback = onRollCallback;
        this.flushInterval = flushInterval;
        this.behaviorOnError = behaviorOnError;
        this.isDlqEnabled = isDlqEnabled;
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
        // Sanitize the file name just be sure and make sure it has the R/W permissions only

        String sanitizedFilePath = FilenameUtils.normalize(filePath);
        if (sanitizedFilePath == null) {
            /*
             * This condition should not occur at all. The files are created in controlled manner with the names consisting DB name, table name. This does not
             * permit names like "../../" or "./" etc. Still adding an additional check.
             */
            String errorMessage = String.format("Exception creating local file for write." +
                    "File %s has a non canonical path", filePath);
            throw new RuntimeException(errorMessage);
        }
        File file = new File(sanitizedFilePath);
        boolean createFile = file.createNewFile(); // if there is a runtime exception. It gets thrown from here
        if (createFile) {
            /*
             * Setting restricted permissions on the file. If these permissions cannot be set, then warn - We cannot fail the ingestion (Failing the ingestion
             * would for not having the permission would mean that there may be data loss or unexpected scenarios.) Added this in a conditional as these
             * permissions can be applied only when the file is created
             *
             */
            try {
                boolean execResult = file.setReadable(true, true);
                execResult = execResult && file.setWritable(true, true);
                execResult = execResult && file.setExecutable(false, false);
                if (!execResult) {
                    log.warn("Setting permissions creating file {} returned false." +
                            "The files set for ingestion can be read by other applications having access." +
                            "Please check security policies on the host that is preventing file permissions from being applied",
                            filePath);
                }
            } catch (Exception ex) {
                // There is a likely chance of the permissions not getting set. This is set to warn
                log.warn("Exception permissions creating file {} returned false." +
                        "The files set for ingestion can be read by other applications having access." +
                        "Please check security policies on the host that is preventing file permissions being applied",
                        filePath, ex);

            }
        }
        // The underlying file is closed only when the current countingStream (abstraction for size based writes) and
        // the file is rolled over
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
            // Since we are using GZIP compression, finish the file. Close is invoked only when this flush finishes
            // and then the file is finished in ingest
            // This is called when there is a time or a size limit reached. The file is then reset/rolled and then a
            // new file is created for processing
            outputStream.finish();
            // It could be we were waiting on the lock when task suddenly stops and we should not ingest anymore
            if (stopped) {
                return;
            }
            try {
                onRollCallback.accept(currentFile);
            } catch (ConnectException e) {
                /*
                 * Swallow the exception and continue to process subsequent records when behavior.on.error is not set to fail mode. Also, throwing/logging the
                 * exception with just a message to avoid polluting logs with duplicate trace.
                 */
                handleErrors("Failed to write records to KustoDB.", e);
            }
            if (delete) {
                dumpFile();
            }
        } else {
            // The stream is closed only when there are non-empty files for ingestion. Note that this closes the
            // FileOutputStream as well
            outputStream.close();
            currentFile = null;
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
        SourceFile temp = currentFile;
        currentFile = null;
        if (temp != null) {
            countingStream.close();
            currentFileDescriptor = null;
            boolean deleted = temp.file.delete();
            if (!deleted) {
                log.warn("couldn't delete temporary file. File exists: " + temp.file.exists());
            }
        }
    }

    public synchronized void rollback() throws IOException {
        if (countingStream != null) {
            countingStream.close();
            if (currentFile != null && currentFile.file != null) {
                dumpFile();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        stop();
    }

    public synchronized void stop() throws DataException {
        stopped = true;
        if (timer != null) {
            Timer temp = timer;
            timer = null;
            temp.cancel();
        }
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
        try (AutoCloseableLock ignored = new AutoCloseableLock(reentrantReadWriteLock.writeLock())) {
            if (stopped) {
                return;
            }
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

    public void writeData(SinkRecord record) throws IOException, DataException {
        if (flushError != null) {
            throw new ConnectException(flushError);
        }
        if (record == null)
            return;
        if (recordWriterProvider == null) {
            initializeRecordWriter(record);
        }
        if (currentFile == null) {
            openFile(record.kafkaOffset());
            resetFlushTimer(true);
        }
        recordWriter.write(record);
        if (this.isDlqEnabled) {
            currentFile.records.add(record);
        }
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
        } else if ((record.valueSchema() != null) && (record.valueSchema().type() == Schema.Type.STRUCT)) {
            if (format.equals(IngestionProperties.DataFormat.JSON) || format.equals(IngestionProperties.DataFormat.MULTIJSON)) {
                recordWriterProvider = new JsonRecordWriterProvider();
            } else if (format.equals(IngestionProperties.DataFormat.AVRO)) {
                recordWriterProvider = new AvroRecordWriterProvider();
            } else {
                throw new ConnectException(String.format("Invalid Kusto table mapping, Kafka records of type "
                        + "Avro and JSON can only be ingested to Kusto table having Avro or JSON mapping. "
                        + "Currently, it is of type %s.", format));
            }
        } else if ((record.valueSchema() == null) || (record.valueSchema().type() == Schema.Type.STRING)) {
            recordWriterProvider = new StringRecordWriterProvider();
        } else if ((record.valueSchema() != null) && (record.valueSchema().type() == Schema.Type.BYTES)) {
            recordWriterProvider = new ByteRecordWriterProvider();
            if (format.equals(IngestionProperties.DataFormat.AVRO)) {
                shouldWriteAvroAsBytes = true;
            }
        } else {
            throw new ConnectException(String.format(
                    "Invalid Kafka record format, connector does not support %s format. This connector supports Avro, Json with schema, Json without schema, Byte, String format. ",
                    record.valueSchema().type()));
        }
    }

    private class CountingOutputStream extends FilterOutputStream {
        private long numBytes = 0;
        private final GZIPOutputStream outputStream;

        CountingOutputStream(GZIPOutputStream out) {
            super(out);
            this.outputStream = out;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            this.numBytes++;
        }

        @Override
        public void write(byte @NotNull [] b) throws IOException {
            out.write(b);
            this.numBytes += b.length;
        }

        @Override
        public void write(byte @NotNull [] b, int off, int len) throws IOException {
            out.write(b, off, len);
            this.numBytes += len;
        }

        public GZIPOutputStream getOutputStream() {
            return this.outputStream;
        }
    }
}
