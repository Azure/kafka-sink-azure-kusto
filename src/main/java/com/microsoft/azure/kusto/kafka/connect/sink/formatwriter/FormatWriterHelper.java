package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
            GenericRecord genericRecord = bytesToAvroRecord(messageBytes);
            if (genericRecord != null) {
                return convertAvroRecordToMap(AVRO_DATA.toConnectSchema(genericRecord.getSchema()), genericRecord);
            } else {
                LOGGER.error("Failed to convert bytes to Avro record. Bytes: {}", ArrayUtils.toString(messageBytes));
                throw new IOException("Unable to convert bytes to AVRO record");
            }
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
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            if (!parser.nextToken().isStructStart()) {
                LOGGER.debug("No start token found for json {}. Is key {} ", json, defaultKeyOrValueField);
                return false;
            }
            OBJECT_MAPPER.readTree(json);
        } catch (IOException e) {
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

    private static @Nullable GenericRecord bytesToAvroRecord(byte[] received_message) throws IOException {
        DatumReader<GenericRecord> avroBytesReader = new GenericDatumReader<>();
        Decoder decoder = DecoderFactory.get().binaryDecoder(received_message, null);
        return avroBytesReader.read(null, decoder);
    }
}
