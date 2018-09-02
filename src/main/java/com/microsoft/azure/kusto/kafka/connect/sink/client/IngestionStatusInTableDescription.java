package com.microsoft.azure.kusto.kafka.connect.sink.client;

public class IngestionStatusInTableDescription {
    public String TableConnectionString;
    public String PartitionKey;
    public String RowKey;
}