package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.avro;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.FormatWriterHelper;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.HeaderAndMetadataWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;


public class AvroRecordWriter extends HeaderAndMetadataWriter implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordWriter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private final String filename;
    private final JsonGenerator writer;
    private Schema schema;

    public AvroRecordWriter(String filename, OutputStream out) {
        this.filename = filename;
        try {
            this.writer = OBJECT_MAPPER.getFactory()
                    .createGenerator(out)
                    .setRootValueSeparator(null);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void write(SinkRecord record) throws IOException {
        try {
            if (schema == null) {
                schema = record.valueSchema();
                LOGGER.debug("Opening record writer for: {}", filename);
            }
            Map<String, Object> updatedValue = new HashMap<>(convertSinkRecordToMap(record, false));
            updatedValue.put(KEYS_FIELD, convertSinkRecordToMap(record,true));
            updatedValue.put(KAFKA_METADATA_FIELD, getKafkaMetaDataAsMap(record));
            writer.writeObject(updatedValue);
            writer.writeRaw(LINE_SEPARATOR);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
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
