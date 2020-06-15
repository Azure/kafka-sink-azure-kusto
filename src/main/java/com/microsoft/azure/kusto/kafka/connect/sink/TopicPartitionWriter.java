package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig.ErrorTolerance;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class TopicPartitionWriter {
  
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    
    private final CompressionType eventDataCompression;
    private final TopicPartition tp;
    private final IngestClient client;
    private final IngestionProperties ingestionProps;
    private final String basePath;
    private final long flushInterval;
    private boolean commitImmediately;
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
    private final ErrorTolerance errorTolerance;

    TopicPartitionWriter(TopicPartition tp, IngestClient client, TopicIngestionProperties ingestionProps, 
        boolean commitImmediatly, KustoSinkConfig config) 
    {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps.ingestionProperties;
        this.fileThreshold = config.getFlushSizeBytes();
        this.basePath = config.getTempDirPath();
        this.flushInterval = config.getFlushInterval();
        this.commitImmediately = commitImmediatly;
        this.currentOffset = 0;
        this.eventDataCompression = ingestionProps.eventDataCompression;
        this.reentrantReadWriteLock = new ReentrantReadWriteLock(true);
        this.maxRetryAttempts = config.getMaxRetryAttempts() + 1; 
        this.retryBackOffTime = config.getRetryBackOffTimeMs();
        this.errorTolerance = config.getErrorTolerance();
        
        if (config.isDlqEnabled()) {
          isDlqEnabled = true;
          dlqTopicName = config.getDlqTopicName();
          Properties properties = new Properties();
          properties.put("bootstrap.servers", config.getDlqBootstrapServers());
          properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
          properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
          kafkaProducer = new KafkaProducer<>(properties);
        } else {
          kafkaProducer = null;
          isDlqEnabled = false;
          dlqTopicName = null;
        }
    }


    public void handleRollFile(SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);

        for (int retryAttempts = 0; true; retryAttempts++) {
            try {
                client.ingestFromFile(fileSourceInfo, ingestionProps);
                log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s)", fileDescriptor.path, fileDescriptor.rawBytes, currentOffset));
                this.lastCommittedOffset = currentOffset;
                return;
            } catch (IngestionClientException exception) {
                //retrying transient exceptions
                backOffForRemainingAttempts(retryAttempts, exception, fileDescriptor);
            } catch (IngestionServiceException exception) {
                // non-retriable and non-transient exceptions
                log.warn("Writing {} failed records to DLQ topic={}", fileDescriptor.records.size(), dlqTopicName);
                fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
                throw new ConnectException("Unable to ingest reccords into KustoDB", exception);
            }
        }
    }

    private void backOffForRemainingAttempts(int retryAttempts, IngestionClientException e, SourceFile fileDescriptor) {
      
        if (retryAttempts < maxRetryAttempts) {
            // RetryUtil can be deleted if exponential backOff is not required, currently using constant backOff.
            // long sleepTimeMs = RetryUtil.computeExponentialBackOffWithJitter(retryAttempts, TimeUnit.SECONDS.toMillis(5));
            long sleepTimeMs = retryBackOffTime;
            log.error("Failed to ingest records into KustoDB, backing off and retrying ingesting records after {} milliseconds.", sleepTimeMs);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
            } catch (InterruptedException interruptedErr) {
                log.warn("Writing {} failed records to DLQ topic={}", fileDescriptor.records.size(), dlqTopicName);
                fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
                throw new ConnectException(String.format("Retrying ingesting records into KustoDB was interuppted after retryAttempts=%s", retryAttempts+1), e);
            }
        } else {
            log.warn("Writing {} failed records to DLQ topic={}", fileDescriptor.records.size(), dlqTopicName);
            fileDescriptor.records.forEach(this::sendFailedRecordToDlq);
            throw new ConnectException("Retry attempts exhausted, failed to ingest records into KustoDB.", e);
        }
    }
    
    public void sendFailedRecordToDlq(SinkRecord record) {
        if (isDlqEnabled) {
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
                          log.error("Failed to write records to DLQ topic={}, exception={}", 
                              dlqTopicName, exception);
                      }
                  });
            } catch (IllegalStateException e) {
                log.error("Failed to write records to DLQ topic, "
                    + "kafka producer has already been closed. Exception={}", e);
            }
        }
    }

    String getFilePath(@Nullable Long offset) {
        // Should be null if flushed by interval
        offset = offset == null ? currentOffset : offset;
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? offset + 1 : offset;

        String compressionExtension = "";
        if (shouldCompressData(ingestionProps, null) || eventDataCompression != null) {
            if(eventDataCompression != null) {
                compressionExtension = "." + eventDataCompression.toString();
            } else {
                compressionExtension = ".gz";
            }
        }

        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s%s", tp.topic(), tp.partition(), nextOffset, ingestionProps.getDataFormat(), compressionExtension)).toString();
    }

    void writeRecord(SinkRecord record) throws ConnectException {
        byte[] value = null;

        // TODO: should probably refactor this code out into a value transformer
        if (record.valueSchema() == null || record.valueSchema().type() == Schema.Type.STRING) {
            value = String.format("%s\n", record.value()).getBytes(StandardCharsets.UTF_8);
        } else if (record.valueSchema().type() == Schema.Type.BYTES) {
            byte[] valueBytes = (byte[]) record.value();
            byte[] separator = "\n".getBytes(StandardCharsets.UTF_8);
            byte[] valueWithSeparator = new byte[valueBytes.length + separator.length];

            System.arraycopy(valueBytes, 0, valueWithSeparator, 0, valueBytes.length);
            System.arraycopy(separator, 0, valueWithSeparator, valueBytes.length, separator.length);

            value = valueWithSeparator;
        } else {
          sendFailedRecordToDlq(record);
          String errorMessage = String.format("KustoSinkConnect can only process records having value type as String or ByteArray. "
              + "Failed to process record with coordinates, topic=%s, partition=%s, offset=%s", record.topic(), record.kafkaPartition(), record.kafkaOffset());
          handleErrors(new DataException(errorMessage), "Unexpected type of kafka record's value");
        }
        if (value == null) {
            this.currentOffset = record.kafkaOffset();
        } else {
            try {
                reentrantReadWriteLock.readLock().lock();

                // Current offset is saved after flushing for the flush timer to use
                fileWriter.write(value, record);
                this.currentOffset = record.kafkaOffset();
            } catch (ConnectException ex) {
                handleErrors(ex, "Failed to ingest records into KustoDB.");
            } catch (IOException ex) {
                handleErrors(ex, "Failed to write records into file for ingestion.");
            } finally {
                reentrantReadWriteLock.readLock().unlock();
            }
        }
    }

    private void handleErrors(Exception ex, String message) {
        if (KustoSinkConfig.ErrorTolerance.NONE == errorTolerance) {
            throw new ConnectException(message, ex);
        } else {
          log.error(String.format("%s, Exception=%s", message, ex));
        }
    }

    void open() {
        // Should compress binary files
        boolean shouldCompressData = shouldCompressData(this.ingestionProps, this.eventDataCompression);

        fileWriter = new FileWriter(
                basePath,
                fileThreshold,
                this::handleRollFile,
                this::getFilePath,
                !shouldCompressData ? 0 : flushInterval,
                shouldCompressData,
                reentrantReadWriteLock);
    }

    void close() {
        try {
            fileWriter.rollback();
            // fileWriter.close(); TODO ?
        } catch (IOException e) {
            log.error("Failed to rollback with exception={}", e);
        }
        try {
            kafkaProducer.close();
        } catch (Exception e) {
            log.error("Failed to close kafka producer={}", e);
        }
    }

    static boolean shouldCompressData(IngestionProperties ingestionProps, CompressionType eventDataCompression) {
        return !(ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.avro.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.parquet.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.orc.toString())
                || eventDataCompression != null);
    }
}
