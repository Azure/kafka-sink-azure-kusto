package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

// TODO tests for byte[]

public abstract class HeaderAndMetadataWriter {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String LINE_SEPARATOR = System.lineSeparator();
    protected static final Logger LOGGER = LoggerFactory.getLogger(KustoRecordWriter.class);
    public String HEADERS_FIELD = "headers";
    public String KEYS_FIELD = "keys";
    public String KEY_FIELD = "key";
    public String VALUE_FIELD = "value";

    public String KAFKA_METADATA_FIELD = "kafka-md";
    public String TOPIC = "topic";
    public String PARTITION = "partition";
    public String OFFSET = "offset";

    @NotNull
    public Map<String, Object> getHeadersAsMap(@NotNull SinkRecord record) {
        Map<String, Object> headers = new HashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), header.value()));
        return headers;
    }

    @NotNull
    @SuppressWarnings(value = "unchecked")
    public Map<String, Object> convertSinkRecordToMap(@NotNull SinkRecord record, boolean isKey) throws IOException {
        Object recordValue = isKey ? record.key() : record.value();
        Schema schema = isKey ? record.keySchema() : record.valueSchema();
        String defaultKeyOrValueField = isKey ? KEY_FIELD : VALUE_FIELD;
        if (recordValue == null) {
            return Collections.emptyMap();
        }
        // Is Avro Data
        if (recordValue instanceof GenericData.Record) {
            return FormatWriterHelper.convertAvroRecordToMap(schema, recordValue);
        }
        // String or JSON
        if (recordValue instanceof String) {
            return FormatWriterHelper.convertStringToMap(recordValue, defaultKeyOrValueField);
        }
        // Map
        if (recordValue instanceof Map) {
            return (Map<String, Object>) recordValue;
        }
        // is a byte array
        if (recordValue instanceof byte[]) {
            return FormatWriterHelper.convertBytesToMap((byte[]) recordValue,defaultKeyOrValueField);
        }
/*
        String fieldName = schema!=null ? StringUtils.defaultIfBlank(schema.name(),
                isKey ? KEY_FIELD : VALUE_FIELD):isKey ? KEY_FIELD : VALUE_FIELD;
*/
        String fieldName = isKey ? KEY_FIELD : VALUE_FIELD;
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
