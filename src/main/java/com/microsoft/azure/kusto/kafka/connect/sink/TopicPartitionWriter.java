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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;
import com.microsoft.azure.kusto.kafka.connect.sink.formatwriter.FormatWriterHelper;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat.*;

public class TopicPartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private static final String COMPRESSION_EXTENSION = ".gz";
    private static final String FILE_EXCEPTION_MESSAGE = "Failed to create file or write record into file for ingestion.";
    private final FormatWriterHelper formatWriterHelper = FormatWriterHelper.getInstance();
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
    private final ReentrantReadWriteLock reentrantReadWriteLock;
    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;

    TopicPartitionWriter(TopicPartition tp, IngestClient client, TopicIngestionProperties ingestionProps,
            KustoSinkConfig config, boolean isDlqEnabled, String dlqTopicName, Producer<byte[], byte[]> dlqProducer) {
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
    }

    static @NotNull String getTempDirectoryName(String tempDirPath) {
        String tempDir = String.format("kusto-sink-connector-%s", UUID.randomUUID());
        Path path = Paths.get(tempDirPath, tempDir).toAbsolutePath();
        return path.toString();
    }

    public void handleRollFile(@NotNull SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);
        /*
         * Since retries can be for a longer duration the Kafka Consumer may leave the group. This will result in a new Consumer reading records from the last
         * committed offset leading to duplication of records in KustoDB. Also, if the error persists, it might also result in duplicate records being written
         * into DLQ topic. Recommendation is to set the following worker configuration as `connector.client.config.override.policy=All` and set the
         * `consumer.override.max.poll.interval.ms` config to a high enough value to avoid consumer leaving the group while the Connector is retrying.
         */
        for (int retryAttempts = 0; true; retryAttempts++) {
            try {
                IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, updateIngestionPropertiesWithTargetFormat());
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
                log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s) " +
                        "to target table (%s) in database (%s)",
                        fileDescriptor.path, fileDescriptor.rawBytes, currentOffset,
                        ingestionProps.ingestionProperties.getTableName(),
                        ingestionProps.ingestionProperties.getDatabaseName()));
                this.lastCommittedOffset = currentOffset;
                return;
            } catch (IngestionServiceException exception) {
                if (ingestionProps.streaming) {
                    Throwable innerException = exception.getCause();
                    if (innerException instanceof KustoDataExceptionBase &&
                            ((KustoDataExceptionBase) innerException).isPermanent()) {
                        throw new ConnectException(exception);
                    }
                }
                // TODO : improve handling of specific transient exceptions once the client supports them.
                // retrying transient exceptions
                log.error("IngestionServiceException when ingesting data into KustoDB, file: {}, database: {}, table: {}, operationId: {}",
                        fileDescriptor.path, ingestionProps.ingestionProperties.getDatabaseName(),
                        ingestionProps.ingestionProperties.getTableName(),
                        ingestionProps.ingestionProperties.getIngestionMapping().getIngestionMappingReference(),exception);
                backOffForRemainingAttempts(retryAttempts, exception, fileDescriptor);
            } catch (IngestionClientException | URISyntaxException exception) {
                log.error("IngestionClientException when ingesting data into KustoDB, file: {}, database: {}, table: {}, operationId: {}",
                        fileDescriptor.path, ingestionProps.ingestionProperties.getDatabaseName(),
                        ingestionProps.ingestionProperties.getTableName(),
                        ingestionProps.ingestionProperties.getIngestionMapping().getIngestionMappingReference(),exception);
                throw new ConnectException(exception);
            }
        }
    }

    private boolean hasStreamingSucceeded(@NotNull IngestionStatus status) {
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

    private void backOffForRemainingAttempts(int retryAttempts, Exception exception, @NotNull SourceFile fileDescriptor) {
        String logMessage = String.format("Writing {%s} failed records to miscellaneous dead-letter queue topic={%s}",
                fileDescriptor.records.size(), dlqTopicName);

        if (retryAttempts < maxRetryAttempts) {
            // RetryUtil can be deleted if exponential backOff is not required, currently using constant backOff.
            // long sleepTimeMs = RetryUtil.computeExponentialBackOffWithJitter(retryAttempts, TimeUnit.SECONDS.toMillis(5));
            long sleepTimeMs = retryBackOffTime;
            log.error("Failed to ingest records into Kusto, backing off and retrying ingesting records " +
                    "after {} milliseconds.", sleepTimeMs);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
            } catch (InterruptedException interruptedErr) {
                if (isDlqEnabled && behaviorOnError != BehaviorOnError.FAIL) {
                    log.warn(logMessage);
                    fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
                }
                throw new ConnectException(String.format("Retrying ingesting records into KustoDB was interrupted " +
                        "after retryAttempts=%s", retryAttempts + 1),
                        exception);
            }
        } else {
            if (isDlqEnabled && behaviorOnError != BehaviorOnError.FAIL) {
                log.warn(logMessage);
                fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
            }
            throw new ConnectException("Retry attempts exhausted, failed to ingest records into KustoDB.", exception);
        }
    }

    public void sendFailedRecordToDlq(@NotNull SinkRecord sinkRecord) {
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
                isDlqEnabled);
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

    private @NotNull IngestionProperties updateIngestionPropertiesWithTargetFormat() {
        IngestionProperties updatedIngestionProperties = new IngestionProperties(this.ingestionProps.ingestionProperties);
        IngestionProperties.DataFormat sourceFormat = ingestionProps.ingestionProperties.getDataFormat();
        if (formatWriterHelper.isSchemaFormat(sourceFormat)) {
            log.debug("Incoming dataformat {}, setting target format to MULTIJSON", sourceFormat);
            updatedIngestionProperties.setDataFormat(MULTIJSON);
        } else {
            updatedIngestionProperties.setDataFormat(ingestionProps.ingestionProperties.getDataFormat());
        }
        // Just to make it clear , split the conditional
        if (formatWriterHelper.isSchemaFormat(sourceFormat)) {
            IngestionMapping mappingReference = ingestionProps.ingestionProperties.getIngestionMapping();
            if (mappingReference != null && StringUtils.isNotEmpty(mappingReference.getIngestionMappingReference())) {
                String ingestionMappingReferenceName = mappingReference.getIngestionMappingReference();
                updatedIngestionProperties.setIngestionMapping(ingestionMappingReferenceName, IngestionMapping.IngestionMappingKind.JSON);
            }
        }
        return updatedIngestionProperties;
    }
}
