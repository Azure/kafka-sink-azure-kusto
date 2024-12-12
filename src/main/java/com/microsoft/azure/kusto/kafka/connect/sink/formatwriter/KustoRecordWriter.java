package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonGenerator;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;

public class KustoRecordWriter extends HeaderAndMetadataWriter implements RecordWriter {
    private final String filename;
    private final JsonGenerator writer;
    private final OutputStream plainOutputStream;
    private Schema schema;

    public KustoRecordWriter(String filename, OutputStream out) {
        this.filename = filename;
        this.plainOutputStream = out;
        try {
            this.writer = OBJECT_MAPPER.getFactory()
                    .createGenerator(out)
                    .setRootValueSeparator(null);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * @param record     the record to persist.
     * @param dataFormat the data format to use.
     * @throws IOException if an error occurs while writing the record.
     */
    @Override
    public void write(SinkRecord record, IngestionProperties.DataFormat dataFormat) throws IOException {
        if (schema == null) {
            schema = record.valueSchema();
            LOGGER.debug("Opening record writer for: {}", filename);
        }
        Map<String, Object> parsedHeaders = getHeadersAsMap(record);
        Map<String, String> kafkaMd = getKafkaMetaDataAsMap(record);
        if (FormatWriterHelper.getInstance().isCsv(dataFormat)) {
            String serializedKeys = StringEscapeUtils.escapeCsv(convertSinkRecordToCsv(record, true));
            String serializedValues = convertSinkRecordToCsv(record, false);
            String serializedHeaders = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(parsedHeaders));
            String serializedMetadata = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(kafkaMd));
            String formattedRecord = String.format("%s,%s,%s,%s", serializedValues, serializedKeys,
                    serializedHeaders, serializedMetadata);
            LOGGER.trace("Writing record to file: Keys {} , Values {} , Headers {} , OverallRecord {}",
                    serializedKeys, serializedValues, serializedHeaders, formattedRecord);
            this.plainOutputStream.write(
                    formattedRecord.getBytes(StandardCharsets.UTF_8));
            this.plainOutputStream.write(LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
        } else {
            Map<String, Object> parsedKeys = convertSinkRecordToMap(record, true, dataFormat).stream().reduce(new HashMap<>(),
                    (acc, map) -> {
                        acc.putAll(map);
                        return acc;
                    });
            Collection<Map<String, Object>> parsedValues = convertSinkRecordToMap(record, false, dataFormat);

            parsedValues.forEach(parsedValue -> {
                Map<String, Object> updatedValue = (record.value() == null) ? new HashMap<>() : new HashMap<>(parsedValue);
                /* Add all the key fields */
                if (record.key() != null) {
                    if (parsedKeys.size() == 1 && parsedKeys.containsKey(KEY_FIELD)) {
                        updatedValue.put(KEYS_FIELD, parsedKeys.get(KEY_FIELD));
                    } else {
                        updatedValue.put(KEYS_FIELD, parsedKeys);
                    }
                }
                /* End add key fields */
                /* Add record headers */
                if (record.headers() != null && !record.headers().isEmpty()) {
                    updatedValue.put(HEADERS_FIELD, parsedHeaders);
                }
                /* End record headers */
                /* Add metadata fields */
                updatedValue.put(KAFKA_METADATA_FIELD, kafkaMd);
                /* End metadata fields */
                try {
                    /* Write out each value row with key and header fields */
                    writer.writeObject(updatedValue);
                    writer.writeRaw(LINE_SEPARATOR);
                } catch (IOException e) {
                    LOGGER.error("Error writing record to file: {}", filename, e);
                    throw new ConnectException(e);
                }
            });
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
            formatWriterHelper.close();
        } catch (IOException e) {
            throw new DataException(e);
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
}
