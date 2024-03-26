package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    private final AvroData avroData = new AvroData(50);

    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new RecordWriter() {
            final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
            Schema updatedSchema;

            @Override
            public void write(SinkRecord record) throws IOException {
                Schema valueSchema = record.valueSchema();
                if (updatedSchema == null) {
                    updatedSchema = createSchemaWithHeaders(valueSchema, record.value()==null);
                    try {
                        log.debug("Opening record writer for: {}", filename);
                        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(updatedSchema);
                        writer.setFlushOnEveryBlock(true);
                        writer.create(avroSchema, out);
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
                Object messageValue = record.value() == null ? null : avroData.fromConnectData(valueSchema, record.value());
                // AvroData wraps primitive types so their schema can be included. We need to unwrap
                // NonRecordContainers to just their value to properly handle these types
                final Struct updatedValue = new Struct(updatedSchema);
                Map<String, Map<String, String>> metadataMap = new HashMap<>();

                if (!record.headers().isEmpty()) {
                    metadataMap.put(HEADERS_FIELD, getHeadersAsMap(record));
                }
                if (!Objects.isNull(record.key())) {
                    metadataMap.put(KEYS_FIELD, getKeysMap(record));
                }
                metadataMap.put(KAFKA_METADATA_FIELD, getKafkaMetaDataAsMap(record));
                updatedValue.put(METADATA_FIELD, metadataMap);
                if(messageValue != null) {
                    if (messageValue instanceof NonRecordContainer) {
                        updatedValue.put(valueSchema.name(), ((NonRecordContainer) messageValue).getValue());
                    } else {
                        if (valueSchema != null) {
                            for (Field field : valueSchema.fields()) {
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
                writer.append(avroData.fromConnectData(updatedSchema, updatedValue));
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


    private Schema createSchemaWithHeaders(Schema baseSchemaStruct, boolean isTombstone) {
        final SchemaBuilder builder = isTombstone ? SchemaBuilder.struct(): SchemaUtil.copySchemaBasics(baseSchemaStruct, SchemaBuilder.struct());
        if(!isTombstone) {
            if (!baseSchemaStruct.type().isPrimitive()) {
                for (Field field : baseSchemaStruct.fields()) {
                    builder.field(field.name(), field.schema());
                }
            } else {
                builder.field(baseSchemaStruct.name(), baseSchemaStruct);
            }
        }
        // Schema will be a map to map of strings
        /*
            {
              "text": "record-0",
              "id": 0,
              "metadata": {
                "headers": {
                  "IntKey": "0",
                  "StringKey": "StringValue"
                },
                "keys": {
                  "id": "0",
                  "_id": "xx1"
                },
                "kafka-md": {
                  "topic": "topic.one",
                  "partition": "1",
                  "offset": "1124"
                }
              }
            }
        */
        builder.field(METADATA_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)));
        return builder.build();
    }
}
