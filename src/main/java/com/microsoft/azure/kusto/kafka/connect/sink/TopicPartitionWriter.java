package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoIngestClient;
import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoIngestionProperties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class TopicPartitionWriter {
    GZIPFileWriter gzipFileWriter;

    TopicPartition tp;
    KustoIngestClient client;

    String databse;
    String table;

    String basePath;
    long fileThreshold;

    long currentOffset;
    Long lastCommitedOffset;

    TopicPartitionWriter(
            TopicPartition tp, KustoIngestClient client,
            String database, String table,
            String basePath, long fileThreshold
    ) {
        this.tp = tp;
        this.client = client;
        this.table = table;
        this.fileThreshold = fileThreshold;
        this.databse = database;
        this.basePath = basePath;
        this.currentOffset = 0;
    }

    public void handleRollFile(GZIPFileDescriptor fileDescriptor) {
        KustoIngestionProperties properties = new KustoIngestionProperties(databse, table, fileDescriptor.rawBytes);

        try {
            client.ingestFromSingleFile(fileDescriptor.path, properties);
            lastCommitedOffset = currentOffset;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getFilePath() {
        long nextOffset = gzipFileWriter != null &&  gzipFileWriter.isDirty() ? currentOffset + 1 : currentOffset;
        return Paths.get(basePath, String.format("kafka_%s_%s_%d", tp.topic(), tp.partition(), nextOffset)).toString();
    }

    public void writeRecord(SinkRecord record) {
        byte[] value = new byte[0];
        // todo: should probably handle more schemas
        if (record.valueSchema() == null || record.valueSchema() == Schema.STRING_SCHEMA) {
            value = record.value().toString().getBytes(StandardCharsets.UTF_8);
        } else if (record.valueSchema() == Schema.BYTES_SCHEMA) {
            value = (byte[]) record.value();
        } else {
            try {
                throw new Exception("Unexpected value type, can only handle strings");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            currentOffset = record.kafkaOffset();

            gzipFileWriter.write(value);

        } catch (IOException e) {
            e.printStackTrace();
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
