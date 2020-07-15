package com.microsoft.azure.kusto.kafka.connect.sink.format;

import com.microsoft.azure.kusto.kafka.connect.sink.CountingOutputStream;

import java.io.OutputStream;

public interface RecordWriterProvider {
  RecordWriter getRecordWriter(String fileName, CountingOutputStream out);
}
