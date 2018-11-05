package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
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
    GZIPFileWriter gzipFileWriter;
    TopicPartition tp;
    IngestClient client;
    IngestionProperties ingestionProps;
    String basePath;
    long fileThreshold;

    long currentOffset;
    Long lastCommittedOffset;

    TopicPartitionWriter(
            TopicPartition tp, IngestClient client, IngestionProperties ingestionProps, String basePath, long fileThreshold
    ) {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps;
        this.fileThreshold = fileThreshold;
        this.basePath = basePath;
        this.currentOffset = 0;
    }

    public void handleRollFile(GZIPFileDescriptor fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);

        try {
            client.ingestFromFile(fileSourceInfo, ingestionProps);
            this.lastCommittedOffset = currentOffset;
        } catch (Exception e) {
            log.error("Ingestion Failed", e);
        }
    }

    public String getFilePath() {
        long nextOffset = gzipFileWriter != null && gzipFileWriter.isDirty() ? currentOffset + 1 : currentOffset;
        return Paths.get(basePath, String.format("kafka_%s_%s_%d", tp.topic(), tp.partition(), nextOffset)).toString();
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
            log.error(String.format("Unexpected value type, skipping record %s", value));
        }

        if (value == null) {
            this.currentOffset = record.kafkaOffset();
        } else {
            try {
                gzipFileWriter.write(value);

                this.currentOffset = record.kafkaOffset();
            } catch (IOException e) {
                log.error("File write failed", e);
            }
        }
    }

    public void open() {
        gzipFileWriter = new GZIPFileWriter(basePath, fileThreshold, this::handleRollFile, this::getFilePath);
    }

    public void close() {
        try {
            gzipFileWriter.rollback();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
