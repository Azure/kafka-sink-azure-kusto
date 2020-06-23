package com.microsoft.azure.kusto.kafka.connect.sink.format;

import java.io.OutputStream;

public interface RecordWriterProvider<C> {
  RecordWriter getRecordWriter(C conf, String fileName, OutputStream out);
}
