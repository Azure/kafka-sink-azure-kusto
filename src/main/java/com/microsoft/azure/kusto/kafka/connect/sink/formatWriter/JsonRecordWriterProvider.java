package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

public class JsonRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);


    private final JsonConverter converter = new JsonConverter();

    public JsonRecordWriterProvider() {
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        converterConfig.put("schemas.cache.size", "50");
        this.converter.configure(converterConfig, false);
    }

    @Override
    public RecordWriter getRecordWriter(final String filename, OutputStream out) {
        try {
            log.debug("Opening record writer for: {}", filename);
            return new RecordWriter() {
                final JsonGenerator writer = OBJECT_MAPPER.getFactory()
                        .createGenerator(out)
                        .setRootValueSeparator(null);

                @Override
                public void write(SinkRecord record) {
                    log.trace("Sink record: {}", record);
                    try {
                        Object value = record.value();
                        Object rawData = (value instanceof Struct) ? extractStruct(record, value) : value;
                        if (rawData != null) {
                            Map<String, Object> serializedValues = OBJECT_MAPPER.readValue(rawData.toString(), MAP_TYPE_REFERENCE);
                            Map<String, Map<String, String>> metadataMap = new HashMap<>();
                            if (!record.headers().isEmpty()) {
                                metadataMap.put(HEADERS_FIELD, getHeadersAsMap(record));
                            }
                            if (!Objects.isNull(record.key())) {
                                metadataMap.put(KEYS_FIELD, getKeysMap(record));
                            }
                            metadataMap.put(KAFKA_METADATA_FIELD, getKafkaMetaDataAsMap(record));
                            serializedValues.put(METADATA_FIELD, metadataMap);
                            out.write(OBJECT_MAPPER.writeValueAsBytes(serializedValues));
                            out.write(LINE_SEPARATOR_BYTES);
                        }
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }

                @Override
                public void commit() {
                    try {
                        writer.flush();
                    } catch (IOException e) {
                        throw new DataException(e);
                    }
                }

                @Override
                public void close() {
                    try {
                        writer.close();
                        out.close();
                    } catch (IOException e) {
                        throw new DataException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    private @Nullable Object extractStruct(SinkRecord record, Object value) throws IOException {
        byte[] rawJson = converter.fromConnectData(record.topic(), record.valueSchema(), value);
        return ArrayUtils.isEmpty(rawJson) ? null : new String(rawJson, StandardCharsets.UTF_8);
    }
}
