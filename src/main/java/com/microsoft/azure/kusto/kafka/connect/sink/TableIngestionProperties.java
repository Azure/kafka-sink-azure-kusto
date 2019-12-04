package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;

class TableIngestionProperties {
    IngestionProperties ingestionProperties = null;
    CompressionType eventDataCompression = null;
}
