package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.avro;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.HeaderAndMetadataWriter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordWriter.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private final AvroData AVRO_DATA = new AvroData(50);
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
            Map<String, Object> updatedValue = new HashMap<>();
            if (schema == null) {
                schema = record.valueSchema();
                LOGGER.debug("Opening record writer for: {}", filename);
                org.apache.avro.Schema avroSchema = AVRO_DATA.fromConnectSchema(schema);
            }
            Object messageValue = record.value() == null ? null : AVRO_DATA.fromConnectData(schema, record.value());
            // AvroData wraps primitive types so their schema can be included. We need to unwrap
            // NonRecordContainers to just their value to properly handle these types
            if (messageValue != null) {
                if (messageValue instanceof NonRecordContainer) {
                    updatedValue.put(schema.name(), ((NonRecordContainer) messageValue).getValue());
                } else {
                    if (schema != null) {
                        for (Field field : schema.fields()) {
                            if (messageValue instanceof Struct) {
                                updatedValue.put(field.name(), ((Struct) messageValue).get(field));
                            } else if (messageValue instanceof GenericData.Record) {
                                updatedValue.put(field.name(), ((GenericData.Record) messageValue).get(field.name()));
                            } else {
                                throw new DataException("Unsupported record type: " + messageValue.getClass());
                            }
                        }
                    }
                }
            }
            updatedValue.put(KEYS_FIELD, getKeysMap(record));
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
