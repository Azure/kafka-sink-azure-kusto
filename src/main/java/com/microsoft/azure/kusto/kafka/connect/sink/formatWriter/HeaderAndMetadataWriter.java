package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;


import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
// TODO tests for byte[]

public abstract class HeaderAndMetadataWriter {
    public String METADATA_FIELD = "metadata";
    public String HEADERS_FIELD = "headers";
    public String KEYS_FIELD = "keys";
    public String KEY_FIELD = "key";
    public String VALUE_FIELD = "value";

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
    public Map<String, Object> convertSinkRecordToMap(@NotNull SinkRecord record, boolean isKey) throws IOException {
        Object recordValue = isKey ? record.key() : record.value();
        Schema schema = isKey ? record.keySchema() : record.valueSchema();
        if(recordValue == null) {
            return Collections.emptyMap();
        }
        // Is Avro Data
        if(recordValue instanceof GenericData.Record) {
            return FormatWriterHelper.convertAvroRecordToMap(schema, recordValue);
        }
        // String or JSON
        if(recordValue instanceof String) {
            return FormatWriterHelper.convertStringToMap(recordValue);
        }
        // is a byte array
        if(recordValue instanceof byte[]) {
            return FormatWriterHelper.convertBytesToMap((byte[])recordValue);
        }
        String fieldName = isKey ? KEY_FIELD : schema.name();
        return Collections.singletonMap(fieldName, recordValue);
    }



    public Map<String, String> getKafkaMetaDataAsMap(@NotNull SinkRecord record) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, record.topic());
        kafkaMetadata.put(PARTITION, String.valueOf(record.kafkaPartition()));
        kafkaMetadata.put(OFFSET, String.valueOf(record.kafkaOffset()));
        return kafkaMetadata;
    }
}
