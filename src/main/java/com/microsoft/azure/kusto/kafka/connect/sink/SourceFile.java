package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

public class SourceFile {
    long rawBytes = 0;
    long numRecords = 0;
    public String path;
    public File file;
    public List<SinkRecord> records = new ArrayList<>();
}