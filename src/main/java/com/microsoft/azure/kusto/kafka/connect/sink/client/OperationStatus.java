package com.microsoft.azure.kusto.kafka.connect.sink.client;

/// <summary>
/// An enum representing the state of a data ingestion operation into Kusto
/// </summary>
public enum OperationStatus {
    /// <summary>
    /// Represents a temporary status.
    /// Might change during the course of ingestion based on the
    /// outcome of the data ingestion operation into Kusto.
    /// </summary>
    Pending,
    /// <summary>
    /// Represents a permanent status.
    /// The data has been successfully ingested to Kusto.
    /// </summary>
    Succeeded,
    /// <summary>
    /// Represents a permanent status.
    /// The data has not been ingested to Kusto.
    /// </summary>
    Failed,
    /// <summary>
    /// Represents a permanent status.
    /// The data has been queued for ingestion.
    /// (This does not indicate the ingestion was successful)
    /// </summary>
    Queued,
    /// <summary>
    /// Represents a permanent status.
    /// No data was supplied for ingestion. The ingest operation was skipped.
    /// </summary>
    Skipped,
    /// <summary>
    /// Represents a permanent status.
    /// Part of the data has been successfully ingested to Kusto while some failed.
    /// </summary>
    PartiallySucceeded
}
