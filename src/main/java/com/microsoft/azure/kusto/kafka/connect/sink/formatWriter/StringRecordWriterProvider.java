package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(StringRecordWriterProvider.class);

    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new RecordWriter() {

            @Override
            public void write(SinkRecord record) throws IOException {
                byte[] value = null;
                value = "%s\n".formatted(record.value()).getBytes(StandardCharsets.UTF_8);
                out.write(value);
            }

            @Override
            public void close() {
                try {
                    out.close();
                } catch (IOException e) {
                    throw new DataException(e);
                }
            }

            @Override
            public void commit() {
                try {
                    out.flush();
                } catch (IOException e) {
                    throw new DataException(e);
                }
            }
        };
    }

}
