package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider.*;


public class AvroRecordWriterTest {
    public static final String TEXT_FIELD_NAME = "text";
    public static final String ID_FIELD_NAME = "id";
    private static ObjectMapper JSON_MAPPER;
    private final String STR_ID_FIELD_NAME = "str-id";

    @BeforeAll
    public static void setUp() {
        JSON_MAPPER = new ObjectMapper();
    }

    @Test
    public void AvroDataWrite() throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        final Schema valueSchema = SchemaBuilder.struct()
                .field(TEXT_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();


        final Schema keySchema = SchemaBuilder.struct()
                .field(STR_ID_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();

        for (int i = 0; i < 10; i++) {
            final Struct valueStruct = new Struct(valueSchema)
                    .put(TEXT_FIELD_NAME, String.format("record-%s", i))
                    .put(ID_FIELD_NAME, i);
            final Struct keyStruct = new Struct(keySchema)
                    .put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i)
                    .put(ID_FIELD_NAME, i);
            SinkRecord recordToWrite = new SinkRecord("avro.test.topic", i % 3, keySchema, keyStruct, valueSchema, valueStruct, i);
            recordToWrite.headers().addString("StringKey", "StringValue");
            recordToWrite.headers().addInt("IntKey", i);
            records.add(recordToWrite);
        }
        String tempAvroFileName = System.getProperty("java.io.tmpdir");
        File file = Paths.get(tempAvroFileName, UUID.randomUUID() + ".avro").toFile();
        Utils.restrictPermissions(file);

        AvroRecordWriterProvider writer = new AvroRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        try {
            validate(file.getPath());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        } finally {
            // FileUtils.deleteQuietly(file);
            System.out.println(file.getPath());
        }
    }

    public void validate(String path) throws IOException, JSONException {
        // Warns if the types are not generified
        GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<>();
        File file = new File(path);
        DataFileReader<GenericData.Record> reader = new DataFileReader<>(file, datum);
        System.out.println("path = " + reader.getSchema());
        GenericData.Record record = new GenericData.Record(reader.getSchema());
        int i = 0;
        while (reader.hasNext()) {
            Map<String, Object> expectedJsonMap = getExpectedResults(i);
            String expectedJson = JSON_MAPPER.writeValueAsString(expectedJsonMap);
            System.out.println("expectedJson = " + expectedJson);
            System.out.println("actualJson = " + reader.next(record).toString());
            JSONAssert.assertEquals(expectedJson, reader.next(record).toString(), false);
            i++;
        }
        reader.close();
    }

    @NotNull
    private Map<String, Object> getExpectedResults(int i) {
        Map<String, Object> expectedJsonMap = new HashMap<>();
        expectedJsonMap.put(ID_FIELD_NAME, i);
        expectedJsonMap.put(TEXT_FIELD_NAME, String.format("record-%s", i));

        Map<String, String> headers = new HashMap<>();
        headers.put("StringKey", "StringValue");
        headers.put("IntKey", String.valueOf(i));

        Map<String, String> keys = new HashMap<>();
        keys.put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i);
        keys.put(ID_FIELD_NAME, String.valueOf(i));

        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put("topic", "avro.test.topic");
        kafkaMetadata.put("partition", String.valueOf(i % 3));
        kafkaMetadata.put("offset", String.valueOf(i));

        expectedJsonMap.put(HEADERS_FIELD, headers);
        expectedJsonMap.put(KEYS_FIELD, keys);
        expectedJsonMap.put(KAFKA_METADATA_FIELD, kafkaMetadata);
        return expectedJsonMap;
    }
}
