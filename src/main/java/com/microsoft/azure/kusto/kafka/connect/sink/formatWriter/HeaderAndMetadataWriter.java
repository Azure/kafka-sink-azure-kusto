package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;


import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
// TODO tests for byte[]

public abstract class HeaderAndMetadataWriter {
    public String METADATA_FIELD = "metadata";
    public String HEADERS_FIELD = "headers";
    public String KEYS_FIELD = "keys";

    public String KAFKA_METADATA_FIELD = "kafka-md";
    public String TOPIC = "topic";
    public String PARTITION = "partition";
    public String OFFSET = "offset";

    @NotNull
    public Map<String, String> getHeadersAsMap(@NotNull SinkRecord record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), header.value().toString()));
        return headers;
    }

    @NotNull
    public Map<String, Object> getKeysMap(@NotNull SinkRecord record) throws IOException {
        Object keyValue = record.key();
        if(keyValue == null) {
            return Collections.emptyMap();
        }
        // Is Avro Data
        if(keyValue instanceof GenericData.Record) {
            return FormatWriterHelper.convertAvroRecordToMap(record.keySchema(), keyValue);
        }
        // String or JSON
        if(keyValue instanceof String) {
            return FormatWriterHelper.convertStringToMap(keyValue);
        }
        // is a byte array
        if(keyValue instanceof byte[]) {
            return FormatWriterHelper.convertBytesToMap((byte[])keyValue);
        }
        return Collections.singletonMap("KEY_FIELD", keyValue);
    }



    public Map<String, String> getKafkaMetaDataAsMap(@NotNull SinkRecord record) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, record.topic());
        kafkaMetadata.put(PARTITION, String.valueOf(record.kafkaPartition()));
        kafkaMetadata.put(OFFSET, String.valueOf(record.kafkaOffset()));
        return kafkaMetadata;
    }
}
