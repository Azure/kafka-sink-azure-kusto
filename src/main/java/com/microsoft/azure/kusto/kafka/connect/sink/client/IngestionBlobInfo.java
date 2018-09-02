package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.util.Map;
import java.util.UUID;

final public class IngestionBlobInfo {
    public String blobPath;
    public Long rawDataSize;
    public String databaseName;
    public String tableName;
    public UUID id;
    public Boolean retainBlobOnSuccess;
    public KustoIngestionProperties.IngestionReportLevel reportLevel;
    public KustoIngestionProperties.IngestionReportMethod reportMethod;
    public Boolean flushImmediately;
    public IngestionStatusInTableDescription IngestionStatusInTable;

    public Map<String, String> additionalProperties;

    public IngestionBlobInfo(String blobPath, String databaseName, String tableName) {
        this.blobPath = blobPath;
        this.databaseName = databaseName;
        this.tableName = tableName;
        id = UUID.randomUUID();
        retainBlobOnSuccess = false;
        flushImmediately = false;
        reportLevel = KustoIngestionProperties.IngestionReportLevel.FailuresOnly;
        reportMethod = KustoIngestionProperties.IngestionReportMethod.Queue;
    }
}
