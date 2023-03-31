package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;

import static com.microsoft.azure.kusto.kafka.connect.sink.Utils.restrictPermissions;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class ByteArrayWriterProviderTest {
    @Test
    public void testByteData() throws IOException {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(new SinkRecord("mytopic", 0, null, null, Schema.BYTES_SCHEMA,
                    String.format("hello-%s", i).getBytes(), i));
        }
        File file = new File("abc.bin");
        restrictPermissions(file);
        file.deleteOnExit();
        try (OutputStream out = Files.newOutputStream(file.toPath());
                BufferedReader br = new BufferedReader(new FileReader(file))) {
            ByteRecordWriterProvider writer = new ByteRecordWriterProvider();
            RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
            for (SinkRecord record : records) {
                rd.write(record);
            }
            rd.commit();
            String st;
            int i = 0;
            while ((st = br.readLine()) != null) {
                assertEquals(st, String.format("hello-%s", i));
                i++;
            }
        }
        FileUtils.deleteQuietly(file);
    }
}
