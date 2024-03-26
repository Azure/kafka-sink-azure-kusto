package com.microsoft.azure.kusto.kafka.connect.sink.format;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public interface RecordWriterProvider {
    String METADATA_FIELD = "metadata";
    String HEADERS_FIELD = "headers";
    String KEYS_FIELD = "keys";
    String KAFKA_METADATA_FIELD = "kafka-md";
    String TOPIC = "topic";
    String PARTITION = "partition";
    String OFFSET = "offset";

    RecordWriter getRecordWriter(String fileName, OutputStream out);

    @NotNull
    default Map<String, String> getHeadersAsMap(@NotNull SinkRecord record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> {
            headers.put(header.key(), header.value().toString());
        });
        return headers;
    }

    @NotNull
    default Map<String, String> getKeysMap(@NotNull SinkRecord record) {
        Map<String, String> keys = new HashMap<>();
        record.keySchema().fields().forEach(field -> {
            String fieldName = field.name();
            if (record.key() instanceof Struct) {
                keys.put(fieldName, ((Struct) record.key()).get(fieldName).toString());
            } else {
                keys.put(fieldName, record.key().toString());
            }
        });
        return keys;
    }


    default Map<String, String> getKafkaMetaDataAsMap(@NotNull SinkRecord record) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, record.topic());
        kafkaMetadata.put(PARTITION, String.valueOf(record.kafkaPartition()));
        kafkaMetadata.put(OFFSET, String.valueOf(record.kafkaOffset()));
        return kafkaMetadata;
    }
}
