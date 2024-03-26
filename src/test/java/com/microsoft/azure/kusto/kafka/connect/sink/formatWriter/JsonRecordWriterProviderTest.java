package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

import static com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider.METADATA_FIELD;

// TODO: Significant duplication among these 4 classes
public class JsonRecordWriterProviderTest extends AbstractRecordWriterTest {
    public static final String JSON_TEST_TOPIC = "json.test.topic";
    private static ObjectMapper JSON_MAPPER;

    @BeforeAll
    public static void setUp() {
        JSON_MAPPER = new ObjectMapper();
    }

    @Test
    public void jsonDataWriteStruct() throws IOException, JSONException {
        String serializedFilePath = this.writeRecordsToJsonFile(
                i-> {
                    try {
                        return JSON_MAPPER.writeValueAsString(new HashMap<String, Object>() {{
                            put(TEXT_FIELD_NAME, String.format(RECORD_FORMAT, i));
                            put(ID_FIELD_NAME, i);
                        }}
                        );
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                },i-> {
                    try {
                        return JSON_MAPPER.writeValueAsString(new HashMap<String, Object>() {{
                                                                  put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i);
                                                                  put(ID_FIELD_NAME, i);
                                                              }}
                        );
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });

        this.validate(serializedFilePath, i -> {
            Map<String, Object> result = new HashMap<>();
            result.put(TEXT_FIELD_NAME, String.format(RECORD_FORMAT, i));
            result.put(ID_FIELD_NAME, i);
            return result;
        });
    }

//    @Test
//    public void jsonDataWriteSimple() throws IOException, JSONException {
//        String serializedFilePath = this.writeRecordsToJsonFile((i) -> i,
//                (i) -> new Struct(schema)
//                        .put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i)
//                        .put(ID_FIELD_NAME, i));
//
//        validate(serializedFilePath, i -> {
//            Map<String, Object> result = new HashMap<>();
//            result.put(ID_FIELD_NAME, i);
//            return result;
//        });
//    }

    public void validate(String path, Function<Integer, Map<String, Object>> resultFunction) throws IOException, JSONException {
        // Warns if the types are not generified
        List<String> actualJsonLines = Files.readAllLines(Paths.get(path));
        int i = 0;

        for (String actualJson : actualJsonLines) {
            Map<String, Object> expectedJsonMap = getExpectedResults(i, resultFunction);
            String expectedJson = JSON_MAPPER.writeValueAsString(expectedJsonMap);
            System.out.println("Expected: " + expectedJson);
            System.out.println("Actual: " + actualJson);
            JSONAssert.assertEquals(expectedJson, actualJson, false);
            i++;
        }
    }

    public String writeRecordsToJsonFile(Function<Integer, Object> keyGenerator,Function<Integer,Object> valueGenerator) throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            Object value = valueGenerator.apply(i);
            Object key = keyGenerator.apply(i);
            SinkRecord recordToWrite = new SinkRecord(JSON_TEST_TOPIC, i % 3, null, key, null, value, i);
            recordToWrite.headers().addString(STRING_KEY, STRING_VALUE);
            recordToWrite.headers().addInt(INT_KEY, i);
            records.add(recordToWrite);
        }
        String tempAvroFileName = System.getProperty("java.io.tmpdir");
        File file = Paths.get(tempAvroFileName, UUID.randomUUID() + ".json").toFile();
        Utils.restrictPermissions(file);

        RecordWriterProvider writer = new JsonRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        out.close();
        return file.getPath();
    }

    @NotNull
    private Map<String, Object> getExpectedResults(int i, @NotNull Function<Integer, Map<String, Object>> resultFunction) {
        Map<String, Object> expectedJsonMap = resultFunction.apply(i);
        Map<String, Map<?, ?>> metadata = getMetaDataMap(i, JSON_TEST_TOPIC);
        expectedJsonMap.put(METADATA_FIELD, metadata);
        return expectedJsonMap;
    }
}
