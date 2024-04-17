package com.microsoft.azure.kusto.kafka.connect.sink.formatwriter;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;

import java.io.OutputStream;

public class KustoRecordWriterProvider implements RecordWriterProvider {
    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new KustoRecordWriter(filename, out);
    }
}
