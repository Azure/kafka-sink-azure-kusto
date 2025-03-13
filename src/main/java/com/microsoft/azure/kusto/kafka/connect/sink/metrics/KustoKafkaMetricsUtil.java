package com.microsoft.azure.kusto.kafka.connect.sink.metrics;

/** All metrics related constants. Mainly for JMX */
public class KustoKafkaMetricsUtil {
  public static final String JMX_METRIC_PREFIX = "kusto.kafka.connector";

  public static final String DLQ_SUB_DOMAIN = "dlq-metrics";
  public static final String DLQ_RECORD_COUNT = "dlqRecordCount";
  public static final String INGESTION_ERROR_COUNT = "ingestionErrorCount";
  public static final String INGESTION_SUCCESS_COUNT = "ingestionSuccessCount";

  // Offset related constants
  public static final String FILE_COUNT_SUB_DOMAIN = "file-counts";
  public static final String FILE_COUNT_ON_INGESTION = "file-count-on-ingestion";
  public static final String FILE_COUNT_ON_STAGE = "file-count-on-stage";
  public static final String FILE_COUNT_PURGED = "file-count-purged";
  public static final String FILE_COUNT_TABLE_STAGE_INGESTION_FAIL = "file-count-table-stage-ingestion-fail";
  public static final String FAILED_TEMP_FILE_DELETIONS = "failed-temp-file-deletions";

  // file count related constants
  public static final String OFFSET_SUB_DOMAIN = "offsets";
  public static final String PROCESSED_OFFSET = "processed-offset";
  public static final String FLUSHED_OFFSET = "flushed-offset";
  public static final String COMMITTED_OFFSET = "committed-offset";
  public static final String PURGED_OFFSET = "purged-offset";

  // Buffer related constants
  public static final String BUFFER_SUB_DOMAIN = "buffer";
  public static final String BUFFER_SIZE_BYTES = "buffer-size-bytes";
  public static final String BUFFER_RECORD_COUNT = "buffer-record-count";

  // Event Latency related constants
  public static final String LATENCY_SUB_DOMAIN = "latencies";

  private KustoKafkaMetricsUtil() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public enum EventType {
    KAFKA_LAG("kafka-lag"),
    COMMIT_LAG("commit-lag"),
    INGESTION_LAG("ingestion-lag");

    private final String metricName;

    EventType(final String metricName) {
      this.metricName = metricName;
    }

    public String getMetricName() {
      return this.metricName;
    }
  }

  public static String constructMetricName(
      final String partitionName, final String subDomain, final String metricName) {
    return String.format("%s/%s/%s", partitionName, subDomain, metricName);
  }
}