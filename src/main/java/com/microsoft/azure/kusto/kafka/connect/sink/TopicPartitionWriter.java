package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.BehaviorOnError;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class TopicPartitionWriter {
  
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private static final String COMPRESSION_EXTENSION = ".gz";
    
    private final TopicPartition tp;
    private final IngestClient client;
    private final IngestionProperties ingestionProps;
    private final String basePath;
    private final long flushInterval;
    private final long fileThreshold;
    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;
    private ReentrantReadWriteLock reentrantReadWriteLock;
    private final long maxRetryAttempts;
    private final long retryBackOffTime;
    private final boolean isDlqEnabled;
    private final String dlqTopicName;
    private final Producer<byte[], byte[]> kafkaProducer;
    private final BehaviorOnError behaviorOnError;

    TopicPartitionWriter(TopicPartition tp, IngestClient client, TopicIngestionProperties ingestionProps,
        KustoSinkConfig config, boolean isDlqEnabled, String dlqTopicName, Producer<byte[], byte[]> dlqProducer)
    {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps.ingestionProperties;
        this.fileThreshold = config.getFlushSizeBytes();
        this.basePath = config.getTempDirPath();
        this.flushInterval = config.getFlushInterval();
        this.currentOffset = 0;
        this.reentrantReadWriteLock = new ReentrantReadWriteLock(true);
        this.maxRetryAttempts = config.getMaxRetryAttempts() + 1; 
        this.retryBackOffTime = config.getRetryBackOffTimeMs();
        this.behaviorOnError = config.getBehaviorOnError();
        this.isDlqEnabled = isDlqEnabled;
        this.dlqTopicName = dlqTopicName;
        this.kafkaProducer = dlqProducer;

    }

    public void handleRollFile(SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);

        /*
         * Since retries can be for a longer duration the Kafka Consumer may leave the group.
         * This will result in a new Consumer reading records from the last committed offset
         * leading to duplication of records in KustoDB. Also, if the error persists, it might also
         * result in duplicate records being written into DLQ topic.
         * Recommendation is to set the following worker configuration as `connector.client.config.override.policy=All`
         * and set the `consumer.override.max.poll.interval.ms` config to a high enough value to
         * avoid consumer leaving the group while the Connector is retrying.
         */
        for (int retryAttempts = 0; true; retryAttempts++) {
            try {
                client.ingestFromFile(fileSourceInfo, ingestionProps);
                log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s)", fileDescriptor.path, fileDescriptor.rawBytes, currentOffset));
                this.lastCommittedOffset = currentOffset;
                return;
            } catch (IngestionServiceException exception) {
                // TODO : improve handling of specific transient exceptions once the client supports them.
                // retrying transient exceptions
                backOffForRemainingAttempts(retryAttempts, exception, fileDescriptor);
            } catch (IngestionClientException exception) {
                throw new ConnectException(exception);
            }
        }
    }

    private void backOffForRemainingAttempts(int retryAttempts, Exception e, SourceFile fileDescriptor) {

        if (retryAttempts < maxRetryAttempts) {
            // RetryUtil can be deleted if exponential backOff is not required, currently using constant backOff.
            // long sleepTimeMs = RetryUtil.computeExponentialBackOffWithJitter(retryAttempts, TimeUnit.SECONDS.toMillis(5));
            long sleepTimeMs = retryBackOffTime;
            log.error("Failed to ingest records into KustoDB, backing off and retrying ingesting records after {} milliseconds.", sleepTimeMs);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
            } catch (InterruptedException interruptedErr) {
                if (isDlqEnabled && behaviorOnError != BehaviorOnError.FAIL) {
                    log.warn("Writing {} failed records to miscellaneous dead-letter queue topic={}", fileDescriptor.records.size(), dlqTopicName);
                    fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
                }
                throw new ConnectException(String.format("Retrying ingesting records into KustoDB was interuppted after retryAttempts=%s", retryAttempts+1), e);
            }
        } else {
            if (isDlqEnabled && behaviorOnError != BehaviorOnError.FAIL) {
                log.warn("Writing {} failed records to miscellaneous dead-letter queue topic={}", fileDescriptor.records.size(), dlqTopicName);
                fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
            }
            throw new ConnectException("Retry attempts exhausted, failed to ingest records into KustoDB.", e);
        }
    }
    
    public void sendFailedRecordToDlq(SinkRecord record) {
        byte[] recordKey = String.format("Failed to write record to KustoDB with the following kafka coordinates, "
            + "topic=%s, partition=%s, offset=%s.", 
            record.topic(), 
            record.kafkaPartition(), 
            record.kafkaOffset()).getBytes(StandardCharsets.UTF_8);
        byte[] recordValue = record.value().toString().getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(dlqTopicName, recordKey, recordValue);
        try {
            kafkaProducer.send(dlqRecord, (recordMetadata, exception) -> {
                  if (exception != null) {
                      throw new KafkaException(
                          String.format("Failed to write records to miscellaneous dead-letter queue topic=%s.", dlqTopicName),
                          exception);
                  }
              });
        } catch (IllegalStateException e) {
            log.error("Failed to write records to miscellaneous dead-letter queue topic, "
                + "kafka producer has already been closed. Exception={}", e);
        }
    }

    String getFilePath(@Nullable Long offset) {
        // Should be null if flushed by interval
        offset = offset == null ? currentOffset : offset;
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? offset + 1 : offset;

        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s%s", tp.topic(), tp.partition(), nextOffset, ingestionProps.getDataFormat(), COMPRESSION_EXTENSION)).toString();
    }

    void writeRecord(SinkRecord record) throws ConnectException {
      if (record == null) {
        this.currentOffset = record.kafkaOffset();
      } else {
        try {
          reentrantReadWriteLock.readLock().lock();
          this.currentOffset = record.kafkaOffset();
          fileWriter.writeData(record);
        } catch (IOException | DataException ex) {
            throw new ConnectException("Failed to create file or write records into file for ingestion.", ex);
        } finally {
          reentrantReadWriteLock.readLock().unlock();
        }
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
                ingestionProps,
                behaviorOnError);
    }

    void close() {
        try {
            fileWriter.rollback();
            // fileWriter.close(); TODO ?
        } catch (IOException e) {
            log.error("Failed to rollback with exception={}", e);
        }
        try {
            if (kafkaProducer != null) {
                kafkaProducer.close();
            }
        } catch (Exception e) {
            log.error("Failed to close kafka producer={}", e);
        }
    }

}
