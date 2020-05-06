package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.File;

public class FileProperties {
    long rawBytes = 0;
    long zippedBytes = 0;
    long numRecords = 0;
    public String path;
    public File file;
}