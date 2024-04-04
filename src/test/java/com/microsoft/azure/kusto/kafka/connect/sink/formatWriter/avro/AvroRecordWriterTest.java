package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.avro;

import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroRecordWriterTest {
    @ParameterizedTest
    //@CsvSource({"avro-simple-schema.json,avro-struct-schema.json"})
    @CsvSource({"avro-struct-schema.json,avro-simple-schema.json"})
    public void AvroDataWrite(String keySchemaPath, String valueSchemaPath) throws IOException {
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

        for (int i = 0; i < 1; i++) {
            Object key = randomAvroKeyData.generate();
            Object value = randomAvroValueData.generate();
            SinkRecord sinkRecord = new SinkRecord("avro.record.topic", i%3,
                    keySchema,
                    key,
                    valueSchema,
                    value,
                    i);
            sinkRecord.headers().addInt("HeaderInt",1);
            sinkRecord.headers().addString("HeaderStr","1");
            records.add(sinkRecord);
//            String json = new String(converter.convertToJson((GenericData.Record)sinkRecord.value()), StandardCharsets.UTF_8);
//            System.out.println("-----------------------------------------------------------------------------");
//            System.out.println(json);
//            System.out.println("-----------------------------------------------------------------------------");
        }
        File file = new File("abc.avro");
        Utils.restrictPermissions(file);
        AvroRecordWriterProvider writer = new AvroRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        validate(file.getPath());
        FileUtils.deleteQuietly(file);
    }

    public void validate(String path) throws IOException {
        // Warns if the types are not generified
        GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<>();
        File file = new File(path);
        DataFileReader<GenericData.Record> reader = new DataFileReader<>(file, datum);
        GenericData.Record record = new GenericData.Record(reader.getSchema());
        int i = 0;
        while (reader.hasNext()) {
            assertEquals(reader.next(record).toString(), String.format("{\"text\": \"record-%s\", \"id\": %s}", i, i));
            i++;
        }
        reader.close();
    }

    // Scenarios to test
    /*
     * SimpleKey , StructValue
     * StructKey , StructValue
     * StructKey , SimpleValue
     * */


}
