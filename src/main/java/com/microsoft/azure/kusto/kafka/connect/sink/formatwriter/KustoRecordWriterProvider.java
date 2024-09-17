package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import java.io.OutputStream;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

public class KustoRecordWriterProvider implements RecordWriterProvider {
    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new KustoRecordWriter(filename, out);
    }
}
