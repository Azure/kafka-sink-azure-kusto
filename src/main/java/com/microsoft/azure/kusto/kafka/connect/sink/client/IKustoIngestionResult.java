package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.net.URISyntaxException;
import java.util.List;

import com.microsoft.azure.storage.StorageException;

public interface IKustoIngestionResult {
    /// <summary>
    /// Retrieves the detailed ingestion status of 
    /// all data ingestion operations into Kusto associated with this IKustoIngestionResult instance.
    /// </summary>
    List<IngestionStatus> GetIngestionStatusCollection() throws StorageException, URISyntaxException;
}
