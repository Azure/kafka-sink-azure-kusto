package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class KustoRecordWriterTest {
    public static final String KEYS = "keys";
    public static final String HEADERS = "headers";
    private static final ObjectMapper RESULT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> GENERIC_MAP = new TypeReference<Map<String, Object>>() {
    };

    @ParameterizedTest
    @CsvSource({
            "avro-simple-schema.json,avro-struct-schema.json,true,false",
            "avro-struct-schema.json,avro-struct-schema.json,false,false",
            "avro-simple-schema.json,avro-simple-schema.json,true,true"
    }
    )
    public void validateAvroDataToBeSerialized(String keySchemaPath, String valueSchemaPath, boolean isSimpleKey, boolean isSimpleValue)
            throws IOException, JSONException {
        List<SinkRecord> records = new ArrayList<>();
        Generator randomAvroValueData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().
                        getResourceAsStream(String.format("avro-schemas/%s", valueSchemaPath)))).build();
        Generator randomAvroKeyData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().
                        getResourceAsStream(String.format("avro-schemas/%s", keySchemaPath)))).build();
        AvroData x = new AvroData(50);
        Schema keySchema = x.toConnectSchema(randomAvroKeyData.schema());
        Schema valueSchema = x.toConnectSchema(randomAvroValueData.schema());
        JsonAvroConverter converter = new JsonAvroConverter();
        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Object key = randomAvroKeyData.generate();
            Object value = randomAvroValueData.generate();
            SinkRecord sinkRecord = new SinkRecord("avro.record.topic", i % 3,
                    keySchema,
                    key,
                    valueSchema,
                    value,
                    i);
            sinkRecord.headers().addInt(String.format("HeaderInt-%s", i), i);
            records.add(sinkRecord);
            String expectedValueString = isSimpleValue ?
                    RESULT_MAPPER.writeValueAsString(Collections.singletonMap("value", value)) :
                    new String(converter.convertToJson((GenericData.Record) value));
            String expectedKeyString = isSimpleKey ?
                    RESULT_MAPPER.writeValueAsString(Collections.singletonMap("key", key)) :
                    new String(converter.convertToJson((GenericData.Record) key));
            String expectedHeaderJson = RESULT_MAPPER.writeValueAsString(Collections.singletonMap(String.format("HeaderInt-%s", i), i));
            expectedResultsMap.put(i, new String[]{expectedHeaderJson, expectedKeyString, expectedValueString});
        }
        File file = new File(String.format("%s.%s", UUID.randomUUID(), "json"));
        Utils.restrictPermissions(file);
        KustoRecordWriterProvider writer = new KustoRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        validate(file.getPath(), expectedResultsMap);
        rd.close();
        FileUtils.deleteQuietly(file);
    }

    private void validate(String actualFilePath, Map<Integer, String[]> expectedResultsMap) throws IOException, JSONException {
        // Warns if the types are not generified
        List<String> actualJson = Files.readAllLines(Paths.get(actualFilePath));
        for (int i = 0; i < actualJson.size(); i++) {
            String actual = actualJson.get(i);
            Map<String, Object> actualMap = RESULT_MAPPER.readValue(actual, GENERIC_MAP);
            String[] expected = expectedResultsMap.get(i);
            String actualKeys = RESULT_MAPPER.writeValueAsString(actualMap.get(KEYS));
            String actualHeaders = RESULT_MAPPER.writeValueAsString(actualMap.get(HEADERS));
            JSONAssert.assertEquals(expected[1], actualKeys, false);
            JSONAssert.assertEquals(expected[0], actualHeaders, false);

            // to get the values it is to remove keys and headers , then get all the fields and compare
            actualMap.remove(KEYS);
            actualMap.remove(HEADERS);
            // Now actualMap contains only the value
            String actualValues = RESULT_MAPPER.writeValueAsString(actualMap);
            JSONAssert.assertEquals(expected[2], actualValues, false);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "avro-simple-schema.json,avro-struct-schema.json,true,false",
//            "avro-struct-schema.json,avro-struct-schema.json,false,false",
//            "avro-simple-schema.json,avro-simple-schema.json,true,true"
    }
    )
    public void validateJsonDataToBeSerialized(String keySchemaPath, String valueSchemaPath, boolean isSimpleKey, boolean isSimpleValue)
            throws IOException, JSONException {
        List<SinkRecord> records = new ArrayList<>();
        Generator randomAvroValueData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().
                        getResourceAsStream(String.format("avro-schemas/%s", valueSchemaPath)))).build();
        Generator randomAvroKeyData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().
                        getResourceAsStream(String.format("avro-schemas/%s", keySchemaPath)))).build();

        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        for (int i = 0; i < 1; i++) {
            Object key = randomAvroKeyData.generate().toString();
            Object value = randomAvroValueData.generate().toString();
            SinkRecord sinkRecord = new SinkRecord("json.record.topic", i % 3,
                    null,
                    key,
                    null,
                    value,
                    i);
            sinkRecord.headers().addInt(String.format("HeaderInt-%s", i), i);
            records.add(sinkRecord);
            String expectedValueString = isSimpleValue ?
                    RESULT_MAPPER.writeValueAsString(Collections.singletonMap("value", value)) :
                    value.toString();
            String expectedKeyString = isSimpleKey ?
                    RESULT_MAPPER.writeValueAsString(Collections.singletonMap("key", key)) :
                    key.toString();
            String expectedHeaderJson = RESULT_MAPPER.writeValueAsString(Collections.singletonMap(String.format("HeaderInt-%s", i), i));
            expectedResultsMap.put(i, new String[]{expectedHeaderJson, expectedKeyString, expectedValueString});
        }
        File file = new File(String.format("%s.%s", UUID.randomUUID(), "json"));
        Utils.restrictPermissions(file);
        KustoRecordWriterProvider writer = new KustoRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        validate(file.getPath(), expectedResultsMap);
        rd.close();
        FileUtils.deleteQuietly(file);
    }
}



