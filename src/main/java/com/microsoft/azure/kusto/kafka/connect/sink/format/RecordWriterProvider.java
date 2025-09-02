package com.microsoft.azure.kusto.kafka.connect.sink.format;

import java.io.OutputStream;

public interface RecordWriterProvider {
    RecordWriter getRecordWriter(String fileName, OutputStream out);
}
