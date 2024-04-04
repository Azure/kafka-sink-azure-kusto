package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter.avro;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

import java.io.OutputStream;

public class AvroRecordWriterProvider implements RecordWriterProvider {

    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new AvroRecordWriter(filename, out);
    }
}
