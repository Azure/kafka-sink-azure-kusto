package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.ingest.IngestionProperties;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat.*;
import static com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat.SINGLEJSON;

public class FormatWriterHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KustoRecordWriter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final AvroData AVRO_DATA = new AvroData(50);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE
            = new TypeReference<Map<String, Object>>() {
    };

    private FormatWriterHelper() {
    }

    public static boolean isSchemaFormat(IngestionProperties.DataFormat dataFormat) {
        return dataFormat == JSON || dataFormat == MULTIJSON
                || dataFormat == AVRO || dataFormat == SINGLEJSON;

    }

    protected static boolean isAvro(IngestionProperties.DataFormat dataFormat) {
        return IngestionProperties.DataFormat.AVRO.equals(dataFormat)
                || IngestionProperties.DataFormat.APACHEAVRO.equals(dataFormat);
    }

    public static boolean isCsv(IngestionProperties.DataFormat dataFormat) {
        return IngestionProperties.DataFormat.CSV.equals(dataFormat);
    }

    public static @NotNull Map<String, Object> convertAvroRecordToMap(Schema schema, Object value) throws IOException {
        Map<String, Object> updatedValue = new HashMap<>();
        if (value != null) {
            if (value instanceof NonRecordContainer) {
                updatedValue.put(schema.name(), ((NonRecordContainer) value).getValue());
            } else {
                if (value instanceof GenericData.Record) {
                    updatedValue.putAll(avroToJson((GenericData.Record) value));
                }
            }
        }
        return updatedValue;
    }

    /**
     * @param messageBytes Raw message bytes to transform
     * @param defaultKeyOrValueField Default value for Key or Value
     * @param dataformat JSON or Avro
     * @return a Map of the K-V of JSON
     */
    public static @NotNull Map<String, Object> convertBytesToMap(byte[] messageBytes,
                                                                 String defaultKeyOrValueField,
                                                                 IngestionProperties.DataFormat dataformat) throws IOException {
        if (messageBytes == null || messageBytes.length == 0) {
            return Collections.emptyMap();
        }
        if (isAvro(dataformat)) {
            return bytesToAvroRecord(defaultKeyOrValueField,messageBytes);
        }
        String bytesAsJson = new String(messageBytes, StandardCharsets.UTF_8);
        if (isJson(dataformat)) {
            return isValidJson(defaultKeyOrValueField,bytesAsJson) ?
                    OBJECT_MAPPER.readerFor(MAP_TYPE_REFERENCE).readValue(bytesAsJson) :
                    Collections.singletonMap(defaultKeyOrValueField,
                            OBJECT_MAPPER.readTree(messageBytes));
        } else {
            return Collections.singletonMap(defaultKeyOrValueField, Base64.getEncoder().encodeToString(messageBytes));
        }
    }

    /**
     * Convert a given avro record to json and return the encoded bytes.
     * @param record The GenericRecord to convert
     */
    private static Map<String, Object> avroToJson(@NotNull GenericRecord record) throws IOException {
        return OBJECT_MAPPER.readerFor(MAP_TYPE_REFERENCE).readValue(record.toString());
    }

    public static @NotNull Map<String, Object> structToMap(@NotNull Struct recordData) {
        List<Field> fields = recordData.schema().fields();
        return fields.stream().collect(Collectors.toMap(Field::name, recordData::get));
    }

    private static boolean isValidJson(String defaultKeyOrValueField, String json) {
        LOGGER.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Step-1");
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            if (!parser.nextToken().isStructStart()) {
                LOGGER.debug("No start token found for json {}. Is key {} ", json, defaultKeyOrValueField);
                return false;
            }
            OBJECT_MAPPER.readTree(json);
        } catch (IOException e) {
            LOGGER.error("An error has occurred",e);
            return false;
        }
        return true;
    }


    public static @NotNull Map<String, Object> convertStringToMap(Object value,
                                                                  String defaultKeyOrValueField,
                                                                  IngestionProperties.DataFormat dataFormat) throws IOException {
        String objStr = (String) value;
        if (isJson(dataFormat) && isValidJson(defaultKeyOrValueField,objStr)) {
            return OBJECT_MAPPER.readerFor(MAP_TYPE_REFERENCE).readValue(objStr);
        } else {
            return Collections.singletonMap(defaultKeyOrValueField, objStr);
        }
    }

    private static boolean isJson(IngestionProperties.DataFormat dataFormat) {
        return IngestionProperties.DataFormat.JSON.equals(dataFormat)
                || IngestionProperties.DataFormat.MULTIJSON.equals(dataFormat)
                || IngestionProperties.DataFormat.SINGLEJSON.equals(dataFormat);
    }

    private static Map<String, Object> bytesToAvroRecord(String defaultKeyOrValueField,byte[] received_message) {
        Map<String, Object> returnValue = new HashMap<>();
        try {
            // avro input parser
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader;
            try {
                dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(received_message), datumReader);
            } catch (Exception e) {
                LOGGER.error("Failed to parse AVRO record(1)\n{}", e.getMessage());
                throw new ConnectException(
                        "Failed to parse AVRO " + "record\n" + e.getMessage());
            }
            while (dataFileReader.hasNext()) {
                String jsonString = dataFileReader.next().toString();
                try {
                    Map<String, Object> nodeMap = OBJECT_MAPPER.readValue(jsonString, MAP_TYPE_REFERENCE);
                    returnValue.putAll(nodeMap);
                } catch (IOException e) {
                    throw new ConnectException(
                            "Failed to parse JSON"
                                    + " "
                                    + "record\nInput String: "
                                    + jsonString
                                    + "\n"
                                    + e.getMessage());
                }
            }
            try {
                dataFileReader.close();
            } catch (IOException e) {
                throw new ConnectException(
                        "Failed to parse AVRO (2) " + "record\n" + e);
            }
            return returnValue;
        } catch (Exception e) {
            LOGGER.error("Failed to parse AVRO record (3) \n", e);
            return Collections.singletonMap(defaultKeyOrValueField, Base64.getEncoder().encodeToString(received_message));
        }
    }
}
