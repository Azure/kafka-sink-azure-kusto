package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import com.microsoft.azure.kusto.kafka.connect.sink.metrics.KustoKafkaMetricsUtil;

public class TopicPartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private static final String COMPRESSION_EXTENSION = ".gz";
    private static final String FILE_EXCEPTION_MESSAGE = "Failed to create file or write record into file for ingestion.";

    private final TopicPartition tp;
    private final IngestClient client;
    private final TopicIngestionProperties ingestionProps;
    private final String basePath;
    private final long flushInterval;
    private final long fileThreshold;
    private final long maxRetryAttempts;
    private final long retryBackOffTime;
    private final boolean isDlqEnabled;
    private final String dlqTopicName;
    private final Producer<byte[], byte[]> dlqProducer;
    private final BehaviorOnError behaviorOnError;
    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;
    private final ReentrantReadWriteLock reentrantReadWriteLock;
    private final MetricRegistry metricRegistry;

    private Counter fileCountOnIngestion;
    private Counter fileCountTableStageIngestionFail;
    private Counter dlqRecordCount;
    private Counter ingestionErrorCount;
    private Counter ingestionSuccessCount;
    private Timer commitLag;
    private Timer ingestionLag;
    private long writeTime;


    TopicPartitionWriter(TopicPartition tp, IngestClient client, TopicIngestionProperties ingestionProps,
                         KustoSinkConfig config, boolean isDlqEnabled, String dlqTopicName, Producer<byte[], byte[]> dlqProducer, MetricRegistry metricRegistry) {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps;
        this.fileThreshold = config.getFlushSizeBytes();
        this.basePath = getTempDirectoryName(config.getTempDirPath());
        this.flushInterval = config.getFlushInterval();
        this.currentOffset = 0;
        this.reentrantReadWriteLock = new ReentrantReadWriteLock(true);
        this.maxRetryAttempts = config.getMaxRetryAttempts() + 1;
        this.retryBackOffTime = config.getRetryBackOffTimeMs();
        this.behaviorOnError = config.getBehaviorOnError();
        this.isDlqEnabled = isDlqEnabled;
        this.dlqTopicName = dlqTopicName;
        this.dlqProducer = dlqProducer;
        this.metricRegistry = metricRegistry;
        initializeMetrics(tp.topic(), metricRegistry);
    }
    private void initializeMetrics(String topic, MetricRegistry metricRegistry) {
        this.fileCountOnIngestion = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, KustoKafkaMetricsUtil.FILE_COUNT_ON_INGESTION));
        this.fileCountTableStageIngestionFail = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, KustoKafkaMetricsUtil.FILE_COUNT_TABLE_STAGE_INGESTION_FAIL));
        this.dlqRecordCount = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.DLQ_SUB_DOMAIN, KustoKafkaMetricsUtil.DLQ_RECORD_COUNT));
        this.ingestionErrorCount = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.DLQ_SUB_DOMAIN, KustoKafkaMetricsUtil.INGESTION_ERROR_COUNT));
        this.ingestionSuccessCount = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.DLQ_SUB_DOMAIN, KustoKafkaMetricsUtil.INGESTION_SUCCESS_COUNT));
        this.commitLag = metricRegistry.timer(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.LATENCY_SUB_DOMAIN, KustoKafkaMetricsUtil.EventType.COMMIT_LAG.getMetricName()));
        this.ingestionLag = metricRegistry.timer(KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.LATENCY_SUB_DOMAIN, KustoKafkaMetricsUtil.EventType.INGESTION_LAG.getMetricName()));
        
        String processedOffsetMetricName = KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.OFFSET_SUB_DOMAIN, KustoKafkaMetricsUtil.PROCESSED_OFFSET);
        if (!metricRegistry.getGauges().containsKey(processedOffsetMetricName)) {
            metricRegistry.register(processedOffsetMetricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return currentOffset;
                }
            });
        }
    
        String committedOffsetMetricName = KustoKafkaMetricsUtil.constructMetricName(topic, KustoKafkaMetricsUtil.OFFSET_SUB_DOMAIN, KustoKafkaMetricsUtil.COMMITTED_OFFSET);
        if (!metricRegistry.getGauges().containsKey(committedOffsetMetricName)) {
            metricRegistry.register(committedOffsetMetricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return lastCommittedOffset != null ? lastCommittedOffset : 0L;
                }
            });
        }
    }

    static String getTempDirectoryName(String tempDirPath) {
        String tempDir = String.format("kusto-sink-connector-%s", UUID.randomUUID());
        Path path = Paths.get(tempDirPath, tempDir).toAbsolutePath();
        return path.toString();
    }

    public void handleRollFile(SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);
        if (writeTime == 0) {
            log.warn("writeTime is not initialized properly before invoking handleRollFile. Setting it to the current time.");
            writeTime = System.currentTimeMillis(); // Initialize writeTime if not already set
        }
        /*
         * Since retries can be for a longer duration the Kafka Consumer may leave the group. This will result in a new Consumer reading records from the last
         * committed offset leading to duplication of records in KustoDB. Also, if the error persists, it might also result in duplicate records being written
         * into DLQ topic. Recommendation is to set the following worker configuration as `connector.client.config.override.policy=All` and set the
         * `consumer.override.max.poll.interval.ms` config to a high enough value to avoid consumer leaving the group while the Connector is retrying.
         */
        fileCountOnIngestion.inc();
        long uploadStartTime = System.currentTimeMillis(); // Record the start time of file upload
        commitLag.update(uploadStartTime - writeTime, TimeUnit.MILLISECONDS);

        for (int retryAttempts = 0; true; retryAttempts++) {
            try {
                IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProps.ingestionProperties);
                if (ingestionProps.streaming && ingestionResult instanceof IngestionStatusResult) {
                    // If IngestionStatusResult returned then the ingestion status is from streaming ingest
                    IngestionStatus ingestionStatus = ingestionResult.getIngestionStatusCollection().get(0);
                    if (!hasStreamingSucceeded(ingestionStatus)) {
                        retryAttempts += ManagedStreamingIngestClient.ATTEMPT_COUNT;
                        backOffForRemainingAttempts(retryAttempts, null, fileDescriptor);
                        log.debug("Kusto ingestion: Streaming of file ({}) of size ({}) at current offset ({}) did NOT succeed; will retry",
                                fileDescriptor.path, fileDescriptor.rawBytes, currentOffset);
                        continue;
                    }
                }
                IngestionStatus ingestionStatus = null;
                if(ingestionResult!=null && ingestionResult.getIngestionStatusCollection()!=null
                        && !ingestionResult.getIngestionStatusCollection().isEmpty()){
                    ingestionStatus = ingestionResult.getIngestionStatusCollection().get(0);
                }
                log.info("Kusto ingestion: file ({}) of size ({}) at current offset ({}) with status ({})",
                        fileDescriptor.path, fileDescriptor.rawBytes, currentOffset,ingestionStatus);
                this.lastCommittedOffset = currentOffset;
                fileCountOnIngestion.dec();
                long ingestionEndTime = System.currentTimeMillis(); // Record the end time of ingestion
                ingestionLag.update(ingestionEndTime - uploadStartTime, TimeUnit.MILLISECONDS); // Update ingestion-lag
                ingestionSuccessCount.inc();
                return;
            } catch (IngestionServiceException exception) {
                fileCountTableStageIngestionFail.inc();
                ingestionErrorCount.inc();
                fileCountOnIngestion.dec();
                if (ingestionProps.streaming) {
                    Throwable innerException = exception.getCause();
                    if (innerException instanceof KustoDataExceptionBase &&
                            ((KustoDataExceptionBase) innerException).isPermanent()) {
                        throw new ConnectException(exception);
                    }
                }
                // TODO : improve handling of specific transient exceptions once the client supports them.
                // retrying transient exceptions
                backOffForRemainingAttempts(retryAttempts, exception, fileDescriptor);
            } catch (IngestionClientException | URISyntaxException exception) {
                fileCountTableStageIngestionFail.inc();
                ingestionErrorCount.inc();
                fileCountOnIngestion.dec();
                throw new ConnectException(exception);
            }
        }
    }

    private boolean hasStreamingSucceeded(IngestionStatus status) {
        switch (status.status) {
            case Succeeded:
            case Queued:
            case Pending:
                return true;
            case Skipped:
            case PartiallySucceeded:
                String failureStatus = status.getFailureStatus();
                String details = status.getDetails();
                UUID ingestionSourceId = status.getIngestionSourceId();
                log.warn("A batch of streaming records has {} ingestion: table:{}, database:{}, operationId: {}," +
                        "ingestionSourceId: {}{}{}.\n" +
                        "Status is final and therefore ingestion won't be retried and data won't reach dlq",
                        status.getStatus(),
                        status.getTable(),
                        status.getDatabase(),
                        status.getOperationId(),
                        ingestionSourceId,
                        (StringUtils.isNotEmpty(failureStatus) ? (", failure: " + failureStatus) : ""),
                        (StringUtils.isNotEmpty(details) ? (", details: " + details) : ""));
                return true;
            case Failed:
        }
        return false;
    }

    private void backOffForRemainingAttempts(int retryAttempts, Exception exception, SourceFile fileDescriptor) {
        if (retryAttempts < maxRetryAttempts) {
            // RetryUtil can be deleted if exponential backOff is not required, currently using constant backOff.
            // long sleepTimeMs = RetryUtil.computeExponentialBackOffWithJitter(retryAttempts, TimeUnit.SECONDS.toMillis(5));
            long sleepTimeMs = retryBackOffTime;
            log.error("Failed to ingest records into Kusto, backing off and retrying ingesting records after {} milliseconds.", sleepTimeMs);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
            } catch (InterruptedException interruptedErr) {
                if (isDlqEnabled && behaviorOnError != BehaviorOnError.FAIL) {
                    log.warn("Writing {} failed records to miscellaneous dead-letter queue topic={}", fileDescriptor.records.size(), dlqTopicName);
                    fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
                }
                throw new ConnectException(String.format("Retrying ingesting records into KustoDB was interrupted after retryAttempts=%s", retryAttempts + 1),
                        exception);
            }
        } else {
            if (isDlqEnabled && behaviorOnError != BehaviorOnError.FAIL) {
                log.warn("Writing {} failed records to miscellaneous dead-letter queue topic={}", fileDescriptor.records.size(), dlqTopicName);
                fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
            }
            throw new ConnectException("Retry attempts exhausted, failed to ingest records into KustoDB.", exception);
        }
    }

    public void sendFailedRecordToDlq(SinkRecord sinkRecord) {
        byte[] recordKey = String.format("Failed to write sinkRecord to KustoDB with the following kafka coordinates, "
                + "topic=%s, partition=%s, offset=%s.",
                sinkRecord.topic(),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset()).getBytes(StandardCharsets.UTF_8);
        byte[] recordValue = sinkRecord.value().toString().getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(dlqTopicName, recordKey, recordValue);
        try {
            dlqProducer.send(dlqRecord, (recordMetadata, exception) -> {
                if (exception != null) {
                    throw new KafkaException(
                            String.format("Failed to write records to miscellaneous dead-letter queue topic=%s.", dlqTopicName),
                            exception);
                }
            });
            dlqRecordCount.inc();
        } catch (IllegalStateException e) {
            log.error("Failed to write records to miscellaneous dead-letter queue topic, "
                    + "kafka producer has already been closed. Exception={0}", e);
        }
    }

    String getFilePath(@Nullable Long offset) {
        // Should be null if flushed by interval
        offset = offset == null ? currentOffset : offset;
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? offset + 1 : offset;

        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s%s", tp.topic(), tp.partition(), nextOffset,
                ingestionProps.ingestionProperties.getDataFormat(), COMPRESSION_EXTENSION)).toString();
    }

    void writeRecord(SinkRecord sinkRecord) throws ConnectException {
        if (sinkRecord != null) {
            try (AutoCloseableLock ignored = new AutoCloseableLock(reentrantReadWriteLock.readLock())) {
                this.currentOffset = sinkRecord.kafkaOffset();
                fileWriter.writeData(sinkRecord);
                // Record the time when data is written to the file
                writeTime = System.currentTimeMillis();
            } catch (IOException | DataException ex) {
                handleErrors(sinkRecord, ex);
            }
        }
    }

    private void handleErrors(SinkRecord sinkRecord, Exception ex) {
        if (BehaviorOnError.FAIL == behaviorOnError) {
            throw new ConnectException(FILE_EXCEPTION_MESSAGE, ex);
        } else if (BehaviorOnError.LOG == behaviorOnError) {
            log.error(FILE_EXCEPTION_MESSAGE, ex);
            sendFailedRecordToDlq(sinkRecord);
        } else {
            log.debug(FILE_EXCEPTION_MESSAGE, ex);
            sendFailedRecordToDlq(sinkRecord);
        }
    }

    void open() {
        // Should compress binary files
        fileWriter = new FileWriter(
                basePath,
                fileThreshold,
                this::handleRollFile,
                this::getFilePath,
                flushInterval,
                reentrantReadWriteLock,
                ingestionProps.ingestionProperties.getDataFormat(),
                behaviorOnError,
                isDlqEnabled,
                tp.topic(),
                metricRegistry);
    }

    void close() {
        try {
            fileWriter.rollback();
            fileWriter.close();
        } catch (IOException e) {
            log.error("Failed to rollback with exception={0}", e);
        }
        try {
            if (dlqProducer != null) {
                dlqProducer.close();
            }
        } catch (Exception e) {
            log.error("Failed to close kafka producer={0}", e);
        }
        try {
            FileUtils.deleteDirectory(new File(basePath));
        } catch (IOException e) {
            log.error("Unable to delete temporary connector folder {}", basePath);
        }
    }

    void stop() {
        fileWriter.stop();
    }
}
