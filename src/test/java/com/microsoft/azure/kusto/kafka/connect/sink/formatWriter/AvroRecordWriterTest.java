package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;

import static com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider.METADATA_FIELD;

public class AvroRecordWriterTest extends AbstractRecordWriterTest {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterTest.class);

    public static final String AVRO_TEST_TOPIC = "avro.test.topic";
    private static ObjectMapper JSON_MAPPER;

    @BeforeAll
    public static void setUp() {
        JSON_MAPPER = new ObjectMapper();
    }

    @Test
    public void avroDataWriteStruct() throws IOException, JSONException {
        final Schema valueSchema = SchemaBuilder.struct()
                .field(TEXT_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();

        final Schema keySchema = SchemaBuilder.struct()
                .field(STR_ID_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();

        String serializedFilePath = this.writeRecordsToAvroFile(valueSchema, keySchema,
                (i, schema) -> new Struct(schema)
                        .put(TEXT_FIELD_NAME, String.format(RECORD_FORMAT, i))
                        .put(ID_FIELD_NAME, i),
                (i, schema) -> new Struct(schema)
                        .put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i)
                        .put(ID_FIELD_NAME, i));

        validate(serializedFilePath, i -> {
            Map<String, Object> result = new HashMap<>();
            result.put(TEXT_FIELD_NAME, String.format(RECORD_FORMAT, i));
            result.put(ID_FIELD_NAME, i);
            return result;
        });
    }

    @Test
    public void avroDataWriteSimple() throws IOException, JSONException {
        final Schema valueSchema = SchemaBuilder.int32().name(ID_FIELD_NAME).build();
        final Schema keySchema = SchemaBuilder.struct()
                .field(STR_ID_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();

        String serializedFilePath = this.writeRecordsToAvroFile(valueSchema, keySchema,
                (i, schema) -> i,
                (i, schema) -> new Struct(schema)
                        .put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i)
                        .put(ID_FIELD_NAME, i));

        validate(serializedFilePath, i -> {
            Map<String, Object> result = new HashMap<>();
            result.put(ID_FIELD_NAME, i);
            return result;
        });
    }

    public void validate(String path, Function<Integer, Map<String, Object>> resultFunction) throws IOException, JSONException {
        // Warns if the types are not generified
        GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<>();
        File file = new File(path);
        DataFileReader<GenericData.Record> reader = new DataFileReader<>(file, datum);
        GenericData.Record record = new GenericData.Record(reader.getSchema());
        int i = 0;
        assert(reader.hasNext());
        while (reader.hasNext()) {
            Map<String, Object> expectedJsonMap = getExpectedResults(i, resultFunction);
            String expectedJson = JSON_MAPPER.writeValueAsString(expectedJsonMap);
            String actualJson = reader.next(record).toString();
            log.debug("Expected: {}" , expectedJson);
            log.debug("Actual: {}" , actualJson);
            JSONAssert.assertEquals(expectedJson, actualJson, false);
            i++;
        }
        reader.close();
    }

    public String writeRecordsToAvroFile(Schema valueSchema, Schema keySchema, BiFunction<Integer,
            Schema, Object> valueGenerator, BiFunction<Integer, Schema, Object> keyGenerator) throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Object value = valueGenerator.apply(i, valueSchema);
            Object key = keyGenerator.apply(i, keySchema);
            SinkRecord recordToWrite = new SinkRecord(AVRO_TEST_TOPIC, i % 3, keySchema, key, valueSchema, value, i);
            recordToWrite.headers().addString(STRING_KEY, STRING_VALUE);
            recordToWrite.headers().addInt(INT_KEY, i);
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
        out.close();
        return file.getPath();
    }

    @Test
    public void avroDataWriteTombstone() throws IOException, JSONException {
        final Schema valueSchema = SchemaBuilder.struct()
                .field(TEXT_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();
        final Schema keySchema = SchemaBuilder.struct()
                .field(STR_ID_FIELD_NAME, SchemaBuilder.string().build())
                .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                .build();

        String serializedTombstonePath = this.writeRecordsToAvroFile(valueSchema, keySchema,
                (i, schema) -> null,
                (i, schema) -> new Struct(schema)
                        .put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i)
                        .put(ID_FIELD_NAME, i));

        validate(serializedTombstonePath, i -> new HashMap<>());
    }

    @NotNull
    private Map<String, Object> getExpectedResults(int i, @NotNull Function<Integer, Map<String, Object>> resultFunction) {
        Map<String, Object> expectedJsonMap = resultFunction.apply(i);
        Map<String, Map<?, ?>> metadata = getMetaDataMap(i, AVRO_TEST_TOPIC);
        expectedJsonMap.put(METADATA_FIELD, metadata);
        return expectedJsonMap;
    }
}
