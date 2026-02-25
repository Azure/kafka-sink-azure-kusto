package com.microsoft.azure.kusto.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

public class TopicPartitionWriterMetricsTest {

    private static final String KUSTO_INGEST_CLUSTER_URL = "https://ingest-cluster.kusto.windows.net";
    private static final String KUSTO_CLUSTER_URL = "https://cluster.kusto.windows.net";
    private static final String DATABASE = "testdb1";
    private static final String TABLE = "testtable1";
    private static final TopicPartition tp = new TopicPartition("testPartition", 11);

    private KustoSinkMetrics metrics;
    private File currentDirectory;
    private KustoSinkConfig config;

    @BeforeEach
    public void setUp() {
        metrics = new KustoSinkMetrics();
        currentDirectory = Utils.getCurrentWorkingDirectory();
        String basePath = Path.of(currentDirectory.getPath(), "testMetrics").toString();
        Map<String, String> settings = new HashMap<>();
        settings.put(KustoSinkConfig.KUSTO_INGEST_URL_CONF, KUSTO_INGEST_CLUSTER_URL);
        settings.put(KustoSinkConfig.KUSTO_ENGINE_URL_CONF, KUSTO_CLUSTER_URL);
        settings.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(KustoSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(KustoSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, basePath);
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_SIZE_BYTES_CONF, "100");
        settings.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "5000");
        config = new KustoSinkConfig(settings);
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) {
            metrics.close();
        }
        FileUtils.deleteQuietly(currentDirectory);
    }

    @Test
    public void testWriteRecordIncrementsRecordsWritten() {
        IngestClient mockClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        props.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);

        TopicPartitionWriter writer = new TopicPartitionWriter(
                tp, mockClient, props, config, false, null, null, metrics);
        writer.open();

        SinkRecord record = new SinkRecord(tp.topic(), tp.partition(), null, null,
                Schema.STRING_SCHEMA, "test,message", 1);
        writer.writeRecord(record);

        assertEquals(1, metrics.getRecordsWritten());
        assertEquals(0, metrics.getRecordsFailed());
        writer.close();
    }

    @Test
    public void testMultipleWriteRecordsIncrementsCounter() {
        IngestClient mockClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        props.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);

        TopicPartitionWriter writer = new TopicPartitionWriter(
                tp, mockClient, props, config, false, null, null, metrics);
        writer.open();

        for (int i = 0; i < 5; i++) {
            SinkRecord record = new SinkRecord(tp.topic(), tp.partition(), null, null,
                    Schema.STRING_SCHEMA, "msg" + i, i);
            writer.writeRecord(record);
        }

        assertEquals(5, metrics.getRecordsWritten());
        writer.close();
    }

    @Test
    public void testHandleRollFileIncrementsIngestionMetrics() throws Exception {
        IngestClient mockClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);

        TopicPartitionWriter writer = new TopicPartitionWriter(
                tp, mockClient, props, config, false, null, null, metrics);

        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        writer.handleRollFile(descriptor);

        assertEquals(1, metrics.getIngestionAttempts());
        assertEquals(1, metrics.getIngestionSuccesses());
        assertEquals(0, metrics.getIngestionFailures());
    }

    @Test
    public void testHandleRollFileClientExceptionIncrementsFailure() throws Exception {
        IngestClient mockClient = mock(IngestClient.class);
        when(mockClient.ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class)))
                .thenThrow(new IngestionClientException("test failure"));

        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);

        TopicPartitionWriter writer = new TopicPartitionWriter(
                tp, mockClient, props, config, false, null, null, metrics);

        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;

        assertThrows(ConnectException.class, () -> writer.handleRollFile(descriptor));
        assertEquals(1, metrics.getIngestionAttempts());
        assertEquals(1, metrics.getIngestionFailures());
        assertEquals(0, metrics.getIngestionSuccesses());
    }

    @Test
    public void testSendFailedRecordToDlqIncrementsCounter() {
        MockProducer<byte[], byte[]> dlqMockProducer = new MockProducer<>(
                true, new RoundRobinPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());

        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);

        TopicPartitionWriter writer = new TopicPartitionWriter(
                tp, mock(IngestClient.class), props, config, true, "dlq.topic", dlqMockProducer, metrics);
        writer.open();

        SinkRecord record = new SinkRecord(tp.topic(), tp.partition(), null, null,
                Schema.STRING_SCHEMA, "failed-message", 1);
        writer.sendFailedRecordToDlq(record);

        assertEquals(1, metrics.getDlqRecordsSent());
        writer.close();
    }

    @Test
    public void testWriterWithoutMetricsDoesNotFail() {
        IngestClient mockClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        props.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);

        // null metrics should not cause any errors
        TopicPartitionWriter writer = new TopicPartitionWriter(
                tp, mockClient, props, config, false, null, null, null);
        writer.open();

        SinkRecord record = new SinkRecord(tp.topic(), tp.partition(), null, null,
                Schema.STRING_SCHEMA, "test,message", 1);
        writer.writeRecord(record);
        writer.close();
    }
}
