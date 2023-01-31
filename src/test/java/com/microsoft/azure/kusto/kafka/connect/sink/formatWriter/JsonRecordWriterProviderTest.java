package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// TODO: Significant duplication among these 4 classes
public class JsonRecordWriterProviderTest {
    @Test
    public void testJsonData() throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("hello", i);
            records.add(new SinkRecord("mytopic", 0, null, null, null, map, i));
        }
        File file = new File("abc.json");
        JsonRecordWriterProvider jsonWriter = new JsonRecordWriterProvider();
        OutputStream out = new FileOutputStream(file);
        RecordWriter rd = jsonWriter.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        int i = 0;
        while ((st = br.readLine()) != null) {
            assertEquals(st, String.format("{\"hello\":%s}", i));
            i++;
        }
        file.delete();
    }

    @Test
    public void testJsonDataNull() throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("hello", i);
            records.add(new SinkRecord("mytopic", 0, null, null, null, map, i));
        }
        records.add(new SinkRecord("mytopic", 0, null, null, null, null, 11));
        File file = new File("abc.json");
        JsonRecordWriterProvider jsonWriter = new JsonRecordWriterProvider();
        OutputStream out = new FileOutputStream(file);
        RecordWriter rd = jsonWriter.getRecordWriter(file.getPath(), out);
        for (SinkRecord record : records) {
            rd.write(record);
        }
        rd.commit();
        BufferedReader br = new BufferedReader(new FileReader(file));
        for(int i = 0; i <= 10; i++){
            String st = br.readLine();
            if(i == 10){
                assertEquals(st,"null");
            }else {
                assertEquals(st, String.format("{\"hello\":%s}", i));
            }
        }
        file.delete();
    }
}
