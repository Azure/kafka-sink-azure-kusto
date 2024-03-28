package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

import static com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider.METADATA_FIELD;

public class JsonRecordWriterProviderTest extends AbstractRecordWriterTest {
    public static final String JSON_TEST_TOPIC = "json.test.topic";
    private static ObjectMapper JSON_MAPPER;

    @BeforeAll
    public static void setUp() {
        JSON_MAPPER = new ObjectMapper();
    }

    private static @NotNull Stream<Arguments> getKeysMapSimpleTypeTestGenerator() {
        return Stream.of(
                Arguments.of("key-1", null, Collections.singletonMap("key", "key-1")),
                Arguments.of(1, null, Collections.singletonMap("key", "1")),
                Arguments.of("{\"keystring\":\"value\"}", null, Collections.singletonMap("keystring", "value")),
                Arguments.of("[1,2,4]", null, Collections.singletonMap("key", "[1,2,4]")),
                Arguments.of("message".getBytes(), null, Collections.singletonMap("key", Base64.getEncoder().encodeToString("message".getBytes()))),
                Arguments.of("{\"str-id\":\"str-1\",\"id\":1}", SchemaBuilder.struct()
                        .field(STR_ID_FIELD_NAME, SchemaBuilder.string().build())
                        .field(ID_FIELD_NAME, SchemaBuilder.int32().build())
                        .build(), new HashMap<String, String>() {{
                    put(STR_ID_FIELD_NAME, "str-1");
                    put(ID_FIELD_NAME, "1");
                }})
        );
    }

    @Test
    public void jsonDataWriteStruct() throws IOException, JSONException {
        String serializedFilePath = this.writeRecordsToJsonFile(
                i -> {
                    try {
                        return JSON_MAPPER.writeValueAsString(new HashMap<String, Object>() {{
                                                                  put(STR_ID_FIELD_NAME, STR_ID_FIELD_NAME + i);
                                                                  put(ID_FIELD_NAME, i);
                                                              }}
                        );
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }, i -> {
                    try {
                        return JSON_MAPPER.writeValueAsString(new HashMap<String, Object>() {{
                                                                  put(TEXT_FIELD_NAME, String.format(RECORD_FORMAT, i));
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

    public void validate(String path, Function<Integer, Map<String, Object>> resultFunction) throws IOException, JSONException {
        // Warns if the types are not generified
        List<String> actualJsonLines = Files.readAllLines(Paths.get(path));
        int i = 0;

        for (String actualJson : actualJsonLines) {
            Map<String, Object> expectedJsonMap = getExpectedResults(i, resultFunction);
            String expectedJson = JSON_MAPPER.writeValueAsString(expectedJsonMap);
            JSONAssert.assertEquals(expectedJson, actualJson, false);
            i++;
        }
    }

    public String writeRecordsToJsonFile(Function<Integer, Object> keyGenerator, Function<Integer, Object> valueGenerator) throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
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

    @Test
    void getRecordWriter() {
    }

    @Test
    void getHeadersAsMap() {
    }

    @ParameterizedTest
    @MethodSource("getKeysMapSimpleTypeTestGenerator")
    void getKeysMapSimpleTypeTest(Object keyValue, Schema keySchema, Map<String, String> expectedResults) {
        JsonRecordWriterProvider recordWriterProvider = new JsonRecordWriterProvider();
        for (int i = 0; i < 10; i++) {
            // testing only the headers here
            SinkRecord recordToWrite = new SinkRecord(JSON_TEST_TOPIC, i % 3, keySchema, keyValue, null, null, i);
            Map<String, String> actualKeyMap = recordWriterProvider.getKeysMap(recordToWrite);
            assert actualKeyMap.equals(expectedResults);
        }
    }

    @Test
    void getKeyObject() {
    }

    @Test
    void getKafkaMetaDataAsMap() {
    }
}
