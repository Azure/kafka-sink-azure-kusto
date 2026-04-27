package com.microsoft.azure.kusto.kafka.connect.sink;

/**
 * MXBean interface for Kusto Sink Connector JMX metrics.
 */
public interface KustoSinkMetricsMXBean {

    long getRecordsWritten();

    long getRecordsFailed();

    long getIngestionAttempts();

    long getIngestionSuccesses();

    long getIngestionFailures();

    long getDlqRecordsSent();
}
