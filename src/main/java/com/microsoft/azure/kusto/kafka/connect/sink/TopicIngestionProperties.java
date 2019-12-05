package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;

class TopicIngestionProperties {

    IngestionProperties ingestionProperties;
    CompressionType eventDataCompression = null;
}
