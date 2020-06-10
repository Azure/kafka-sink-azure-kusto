package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private final CompressionType eventDataCompression;
    private final TopicPartition tp;
    private final IngestClient client;
    private final IngestionProperties ingestionProps;
    private final String basePath;
    private final long flushInterval;
    private boolean commitImmediately;
    private final long fileThreshold;
    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;
    private int defaultRetriesCount;
    private int currentRetries;
    private ReentrantReadWriteLock reentrantReadWriteLock;

    TopicPartitionWriter(TopicPartition tp, IngestClient client, TopicIngestionProperties ingestionProps, String basePath,
                         long fileThreshold, long flushInterval, boolean commitImmediatly, int retriesCount) {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps.ingestionProperties;
        this.fileThreshold = fileThreshold;
        this.basePath = basePath;
        this.flushInterval = flushInterval;
        this.commitImmediately = commitImmediatly;
        this.currentOffset = 0;
        this.eventDataCompression = ingestionProps.eventDataCompression;
        this.defaultRetriesCount = retriesCount;
        this.currentRetries = retriesCount;
        this.reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    }

    String handleRollFile(SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);
        new ArrayList<>();
        try {
            client.ingestFromFile(fileSourceInfo, ingestionProps);

            log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s)", fileDescriptor.path, fileDescriptor.rawBytes, currentOffset));
            this.lastCommittedOffset = currentOffset;
            currentRetries = defaultRetriesCount;
        } catch (Exception e) {
            log.error("Ingestion Failed for file : "+ fileDescriptor.file.getName() + ", message: " + e.getMessage() + "\nException  : " + ExceptionUtils.getStackTrace(e));
            if (commitImmediately) {
                if (currentRetries > 0) {
                    try {
                        // Default time for commit is 5 seconds timeout.
                        Thread.sleep(1500);
                    } catch (InterruptedException e1) {
                        log.error("Couldn't sleep !");
                    }
                    log.error("Ingestion Failed for file : " + fileDescriptor.file.getName() + ", defaultRetriesCount left '" + defaultRetriesCount + "'. message: " + e.getMessage() + "\nException  : " + ExceptionUtils.getStackTrace(e));
                    currentRetries--;
                    return handleRollFile(fileDescriptor);
                } else {
                  currentRetries = defaultRetriesCount;

                  // Returning string will make the caller throw
                  return "Ingestion Failed for file : " + fileDescriptor.file.getName() + ", defaultRetriesCount left '" + defaultRetriesCount + "'. message: " + e.getMessage() + "\nException  : " + ExceptionUtils.getStackTrace(e);
                }
            }
        }

        return null;
    }

    String getFilePath(@Nullable Long offset) {
        // Should be null if flushed by interval
        offset = offset == null ? currentOffset : offset;
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? offset + 1 : offset;

        String compressionExtension = "";
        if (shouldCompressData(ingestionProps, null) || eventDataCompression != null) {
            if(eventDataCompression != null) {
                compressionExtension = "." + eventDataCompression.toString();
            } else {
                compressionExtension = ".gz";
            }
        }

        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s%s", tp.topic(), tp.partition(), nextOffset, ingestionProps.getDataFormat(), compressionExtension)).toString();
    }

    void writeRecord(SinkRecord record) throws ConnectException {
        byte[] value = null;

        // TODO: should probably refactor this code out into a value transformer
        if (record.valueSchema() == null || record.valueSchema().type() == Schema.Type.STRING) {
            value = String.format("%s\n", record.value()).getBytes(StandardCharsets.UTF_8);
        } else if (record.valueSchema().type() == Schema.Type.BYTES) {
            byte[] valueBytes = (byte[]) record.value();
            byte[] separator = "\n".getBytes(StandardCharsets.UTF_8);
            byte[] valueWithSeparator = new byte[valueBytes.length + separator.length];

            System.arraycopy(valueBytes, 0, valueWithSeparator, 0, valueBytes.length);
            System.arraycopy(separator, 0, valueWithSeparator, valueBytes.length, separator.length);

            value = valueWithSeparator;
        } else {
            log.error(String.format("Unexpected value type, skipping record %s", record));
        }
        if (value == null) {
            this.currentOffset = record.kafkaOffset();
        } else {
            try {
                reentrantReadWriteLock.readLock().lock();

                // Current offset is saved after flushing for the flush timer to use
                fileWriter.write(value, record.kafkaOffset());
                this.currentOffset = record.kafkaOffset();
            } catch (ConnectException ex) {
                if (commitImmediately) {
                    throw ex;
                }
            } catch (IOException ex) {
                if (commitImmediately) {
                    throw new ConnectException("Got an IOExcption while writing to file with message:" + ex.getMessage());
                }
            } finally {
                reentrantReadWriteLock.readLock().unlock();
            }
        }
    }

    void open() {
        // Should compress binary files
        boolean shouldCompressData = shouldCompressData(this.ingestionProps, this.eventDataCompression);

        fileWriter = new FileWriter(
                basePath,
                fileThreshold,
                this::handleRollFile,
                this::getFilePath,
                !shouldCompressData ? 0 : flushInterval,
                shouldCompressData,
                reentrantReadWriteLock);
    }

    void close() {
        try {
            fileWriter.rollback();
            // fileWriter.close(); TODO ?
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static boolean shouldCompressData(IngestionProperties ingestionProps, CompressionType eventDataCompression) {
        return !(ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.avro.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.parquet.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.orc.toString())
                || eventDataCompression != null);
    }
}
