package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import static com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider.*;

public abstract class AbstractRecordWriterTest {
    public static final String TEXT_FIELD_NAME = "text";
    public static final String ID_FIELD_NAME = "id";
    public static final String STRING_KEY = "StringKey";
    public static final String STRING_VALUE = "StringValue";
    public static final String INT_KEY = "IntKey";
    public static final String RECORD_FORMAT = "record-%s";
    static final String STR_ID_FIELD_NAME = "str-id";

    @NotNull
    private static Map<String, String> getKafkaMetadata(int i, String topicName) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, topicName);
        kafkaMetadata.put(PARTITION, String.valueOf(i % 3));
        kafkaMetadata.put(OFFSET, String.valueOf(i));
        return kafkaMetadata;
    }

    @NotNull
    private static Map<String, String> getHeaders(int i) {
        Map<String, String> headers = new HashMap<>();
        headers.put(STRING_KEY, STRING_VALUE);
        headers.put(INT_KEY, String.valueOf(i));
        return headers;
    }

    @NotNull
    Map<String, Map<?, ?>> getMetaDataMap(int i, String topic) {
        Map<String, Map<?, ?>> metadata = new HashMap<>();
        Map<String, String> headers = getHeaders(i);
        Map<String, String> keys = getKeyFields(i);
        Map<String, String> kafkaMetadata = getKafkaMetadata(i, topic);
        metadata.put(HEADERS_FIELD, headers);
        metadata.put(KEYS_FIELD, keys);
        metadata.put(KAFKA_METADATA_FIELD, kafkaMetadata);
        return metadata;
    }

    @NotNull
    private Map<String, String> getKeyFields(int i) {
        Map<String, String> keys = new HashMap<>();
        keys.put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i);
        keys.put(ID_FIELD_NAME, String.valueOf(i));
        return keys;
    }
}
