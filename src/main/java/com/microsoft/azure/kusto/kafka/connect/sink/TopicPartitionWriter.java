package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private final CompressionType compressionType;
    private final TopicPartition tp;
    private final IngestClient client;
    private final IngestionProperties ingestionProps;
    private final String basePath;
    private final long flushInterval;
    private final long fileThreshold;

    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;

    TopicPartitionWriter(TopicPartition tp, IngestClient client, TableIngestionProperties ingestionProps, String basePath, long fileThreshold, long flushInterval) {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps.ingestionProperties;
        this.fileThreshold = fileThreshold;
        this.basePath = basePath;
        this.flushInterval = flushInterval;
        this.currentOffset = 0;
        this.compressionType = ingestionProps.eventDataCompression;
    }

    public void handleRollFile(FileDescriptor fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);

        try {
            client.ingestFromFile(fileSourceInfo, ingestionProps);
            log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s)", fileDescriptor.path, fileDescriptor.rawBytes, currentOffset));
            this.lastCommittedOffset = currentOffset;
        } catch (Exception e) {
            log.error("Ingestion Failed for file : "+ fileDescriptor.file.getName() + ", message: " + e.getMessage() + "\nException  : " + ExceptionUtils.getStackTrace(e));
        }
    }

    public String getFilePath() {
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? currentOffset + 1 : currentOffset;

        // Output files are always compressed
        String compressionExtension = this.compressionType == null ? "gz" : this.compressionType.toString();
        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s.%s", tp.topic(), tp.partition(), nextOffset, ingestionProps.getDataFormat(), compressionExtension)).toString();
    }

    public void writeRecord(SinkRecord record) {
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
                fileWriter.write(value);

                this.currentOffset = record.kafkaOffset();
            } catch (IOException e) {
                log.error("File write failed", e);
            }
        }
    }

    public void open() {
        boolean flushImmediately = ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.avro.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.parquet.toString())
                || this.compressionType != null;

        fileWriter = new FileWriter(
                basePath,
                fileThreshold,
                this::handleRollFile,
                this::getFilePath,
                flushImmediately ? 0 : flushInterval,
                this.compressionType == null);
    }

    public void close() {
        try {
            fileWriter.rollback();
            // fileWriter.close(); TODO ?
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
