package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class AvroRecordWriterProvider implements RecordWriterProvider {
  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private final AvroData avroData = new AvroData(50);

  @Override
  public RecordWriter getRecordWriter(String filename, OutputStream out) {
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      Schema schema;

      @Override
      public void write(SinkRecord record) throws IOException {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.debug("Opening record writer for: {}", filename);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer.setFlushOnEveryBlock(true);
            writer.create(avroSchema, out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (value instanceof NonRecordContainer) {
            writer.append(((NonRecordContainer) value).getValue());
          } else {
            writer.append(value);
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
    };
  }
}
