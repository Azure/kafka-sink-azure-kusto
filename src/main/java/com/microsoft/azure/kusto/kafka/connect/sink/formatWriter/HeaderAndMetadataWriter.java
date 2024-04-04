package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.*;

public abstract class HeaderAndMetadataWriter {
    public String METADATA_FIELD = "metadata";
    public String HEADERS_FIELD = "headers";
    public String KEYS_FIELD = "keys";
    public String KEY_FIELD = "key";
    public String KAFKA_METADATA_FIELD = "kafka-md";
    public String TOPIC = "topic";
    public String PARTITION = "partition";
    public String OFFSET = "offset";
    private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE
            = new TypeReference<Map<String, Object>>() {
    };

    @NotNull
    public Map<String, String> getHeadersAsMap(@NotNull SinkRecord record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), header.value().toString()));
        return headers;
    }

    @NotNull
    public Map<String, Object> getKeysMap(@NotNull SinkRecord record) {
        Map<String, Object> keys = new HashMap<>();
        if (record.key() == null) {
            return keys;
        }
        Object key = record.key();
        if (record.keySchema() != null && record.keySchema() instanceof Struct) {
            record.keySchema().fields().forEach(field -> {
                String fieldName = field.name();
                if (record.key() instanceof Struct) {
                    keys.put(fieldName, ((Struct) record.key()).get(fieldName));
                } else {
                    keys.put(fieldName, record.key());
                }
            });
        } else {
            // Key is not null, but key schema is null
            final Schema.Type schemaType = ConnectSchema.schemaType(record.key().getClass());
            switch (schemaType) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case ARRAY:
                    keys.put(KEY_FIELD, String.valueOf(key));
                    break;
                case BYTES:
                    keys.put(KEY_FIELD, Base64.getEncoder().encodeToString((byte[]) key));
                    break;
                case STRING:
                    getKeyObject(key.toString()).forEach((k, v) -> keys.put(k,
                            Objects.toString(v)));
                    break;
                case MAP:
                    Map<?, ?> mapFields = (Map<?, ?>) key;
                    if (mapFields != null) {
                        for (Map.Entry<?, ?> entry : mapFields.entrySet()) {
                            if (entry.getKey() != null && entry.getValue() != null) {
                                keys.put(entry.getKey().toString(), entry.getValue().toString());
                            }
                        }
                    }
                    break;
                case STRUCT:
                    Struct keyStruct = (Struct) key;
                    if (keyStruct != null && keyStruct.schema() != null) {
                        keyStruct.schema().fields().forEach(field -> {
                            String fieldName = field.name();
                            if (keyStruct.get(fieldName) != null) {
                                keys.put(fieldName, keyStruct.get(fieldName).toString());
                            }
                        });
                    }
                    break;
                default:
                    throw new DataException(schemaType.name() + " is not supported as the document id.");
            }
        }
        return keys;
    }

    public Map<String, Object> getKeyObject(@NotNull String keyValue) {
        try {
            return OBJECT_MAPPER.readValue(keyValue, MAP_TYPE_REFERENCE);
        } catch (JsonProcessingException e) {
            return Collections.singletonMap(KEY_FIELD, keyValue);
        }
    }


    public Map<String, String> getKafkaMetaDataAsMap(@NotNull SinkRecord record) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, record.topic());
        kafkaMetadata.put(PARTITION, String.valueOf(record.kafkaPartition()));
        kafkaMetadata.put(OFFSET, String.valueOf(record.kafkaOffset()));
        return kafkaMetadata;
    }
}
