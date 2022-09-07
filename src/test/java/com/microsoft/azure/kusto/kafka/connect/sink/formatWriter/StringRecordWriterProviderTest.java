package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringRecordWriterProviderTest {
    @Test
    public void testStringData() throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(new SinkRecord("mytopic", 0, null, null, Schema.STRING_SCHEMA, String.format("hello-%s", i), i));
        }
        File file = new File("abc.txt");
        StringRecordWriterProvider writer = new StringRecordWriterProvider();
        OutputStream out = new FileOutputStream(file);
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        int i = 0;
        while ((st = br.readLine()) != null) {
            assertEquals(st, String.format("hello-%s", i));
            i++;
        }
        file.delete();
    }
}
