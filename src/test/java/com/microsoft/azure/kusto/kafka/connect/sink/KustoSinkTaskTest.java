package com.microsoft.azure.kusto.kafka.connect.sink;

import static com.microsoft.azure.kusto.kafka.connect.sink.Utils.getCurrentWorkingDirectory;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import org.apache.kafka.common.config.ConfigException;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class KustoSinkTaskTest {
    File currentDirectory;

    @BeforeEach
    public final void before() {
        currentDirectory = getCurrentWorkingDirectory();
    }

    @AfterEach
    public final void after() {
        FileUtils.deleteQuietly(currentDirectory);
    }

    @Test
    public void testSinkTaskOpen() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic1", 2));
        tps.add(new TopicPartition("topic2", 1));
        kustoSinkTaskSpy.open(tps);
        assertEquals(3, kustoSinkTaskSpy.writers.size());
    }

    @Test
    public void testSinkTaskPutRecord() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition tp = new TopicPartition("topic1", 1);
        tps.add(tp);
        kustoSinkTaskSpy.open(tps);
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), 10));
        kustoSinkTaskSpy.put(records);
        assertEquals(10, kustoSinkTaskSpy.writers.get(tp).currentOffset);
    }

    @Test
    public void testSinkTaskPutRecordMissingPartition() {
        try {
            HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
            configs.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, System.getProperty("java.io.tmpdir"));
            KustoSinkTask kustoSinkTask = new KustoSinkTask();
            KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
            doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
            kustoSinkTaskSpy.start(configs);
            ArrayList<TopicPartition> tps = new ArrayList<>();
            tps.add(new TopicPartition("topic1", 1));
            kustoSinkTaskSpy.open(tps);
            List<SinkRecord> records = new ArrayList<>();
            records.add(
                    new SinkRecord("topic2", 1, null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8),
                            10));
            Throwable exception = assertThrows(ConnectException.class, () -> kustoSinkTaskSpy.put(records));
            assertEquals("Received a record without a mapped writer for topic:partition(topic2:1), dropping record.",
                    exception.getMessage());
        } catch (Exception ex) {
            // Accessors to system property may fail with runtime faults.
            fail(ex);
        }
    }

    @Test
    public void getTable() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        {
            // single table mapping should cause all topics to be mapped to a single table
            assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.CSV, kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDataFormat());
            assertEquals("Mapping", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getIngestionMapping().getIngestionMappingReference());
            assertEquals("db2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            assertEquals("table2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.MULTIJSON, kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDataFormat());
            Assertions.assertNull(kustoSinkTaskSpy.getIngestionProps("topic3"));
        }
    }

    @Test
    public void getWildcardTable() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF,
                "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': '*','db': 'dbWildcard', 'table': 'tableWildcard','format': 'json'}]");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        {
            // explicit mapping should be preferred
            assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());

            // wildcard mapping should be used for other topics
            assertEquals("dbWildcard", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            assertEquals("tableWildcard", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            assertEquals("dbWildcard", kustoSinkTaskSpy.getIngestionProps("anyTopic").ingestionProperties.getDatabaseName());
            assertEquals("tableWildcard", kustoSinkTaskSpy.getIngestionProps("anyTopic").ingestionProperties.getTableName());
        }
    }

    @Test
    public void getTableWithoutMapping() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        {
            // single table mapping should cause all topics to be mapped to a single table
            assertEquals("db1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDatabaseName());
            assertEquals("table1", kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.CSV, kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties.getDataFormat());
            assertEquals("db2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDatabaseName());
            assertEquals("table2", kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getTableName());
            assertEquals(IngestionProperties.DataFormat.MULTIJSON, kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties.getDataFormat());
            Assertions.assertNull(kustoSinkTaskSpy.getIngestionProps("topic3"));
        }
    }

    @Test
    public void closeTaskAndWaitToFinish() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic2", 2));
        tps.add(new TopicPartition("topic2", 3));
        kustoSinkTaskSpy.open(tps);
        // Clean fast close
        long l1 = System.currentTimeMillis();
        kustoSinkTaskSpy.close(tps);
        long l2 = System.currentTimeMillis();
        assertTrue(l2 - l1 < 1000);
        // Check close time when one close takes time to close
        TopicPartition tp = new TopicPartition("topic2", 4);
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = kustoSinkTaskSpy.getIngestionProps("topic2").ingestionProperties;
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, new KustoSinkConfig(configs), false, null, null);
        TopicPartitionWriter writerSpy = spy(writer);
        long sleepTime = 2 * 1000;
        Answer<Void> answer = invocation -> {
            Thread.sleep(sleepTime);
            return null;
        };
        doAnswer(answer).when(writerSpy).close();
        kustoSinkTaskSpy.open(tps);
        writerSpy.open();
        tps.add(tp);
        kustoSinkTaskSpy.writers.put(tp, writerSpy);
        kustoSinkTaskSpy.close(tps);
        long l3 = System.currentTimeMillis();
        System.out.println("l3-l2 " + (l3 - l2));
        assertTrue(l3 - l2 > sleepTime && l3 - l2 < sleepTime + 1000);
    }

    @Test
    public void testCreateKustoEngineClient() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        KustoSinkConfig kustoSinkConfig = new KustoSinkConfig(configs);
        Client client = KustoSinkTask.createKustoEngineClient(kustoSinkConfig);
        Assertions.assertNotNull(client);
    }

    @Test
    public void testValidateTableMappingsConnectError() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        configs.put(KustoSinkConfig.KUSTO_SINK_ENABLE_TABLE_VALIDATION, "true");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        KustoSinkConfig kustoSinkConfig = new KustoSinkConfig(configs);
        assertThrows(ConnectException.class, () -> kustoSinkTask.validateTableMappings(kustoSinkConfig));
    }

    @Test
    public void testStopSuccess() throws IOException {
        // set-up
        // easy to set it this way than mock
        TopicPartition mockPartition = new TopicPartition("test-topic", 0);
        TopicPartitionWriter mockPartitionWriter = mock(TopicPartitionWriter.class);
        doNothing().when(mockPartitionWriter).close();
        IngestClient mockClient = mock(IngestClient.class);
        doNothing().when(mockClient).close();
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        // There is no mutate constructor
        kustoSinkTask.writers = Collections.singletonMap(mockPartition, mockPartitionWriter);
        kustoSinkTask.kustoIngestClient = mockClient;

        // when
        kustoSinkTask.stop();

        // then
        verify(mockClient, times(1)).close();
        verify(mockPartitionWriter, times(1)).close();
    }

    @Test
    public void precommitDoesNotCommitNewerOffsets() throws InterruptedException {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "100");
        KustoSinkTask kustoSinkTask = new KustoSinkTask();
        KustoSinkTask kustoSinkTaskSpy = spy(kustoSinkTask);
        doNothing().when(kustoSinkTaskSpy).validateTableMappings(Mockito.any());
        kustoSinkTaskSpy.start(configs);
        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition topic1 = new TopicPartition("topic1", 1);
        tps.add(topic1);
        kustoSinkTaskSpy.open(tps);
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = kustoSinkTaskSpy.getIngestionProps("topic1").ingestionProperties;
        TopicPartitionWriter topicPartitionWriterSpy = spy(
                new TopicPartitionWriter(topic1, mockedClient, props, new KustoSinkConfig(configs), false, null, null));
        topicPartitionWriterSpy.open();
        kustoSinkTaskSpy.writers.put(topic1, topicPartitionWriterSpy);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Stopped stoppedObj = new Stopped();
        AtomicInteger offset = new AtomicInteger(1);
        Runnable insertExec = () -> {
            while (!stoppedObj.stopped) {
                List<SinkRecord> records = new ArrayList<>();

                records.add(new SinkRecord("topic1", 1, null, null, null, "stringy message".getBytes(StandardCharsets.UTF_8), offset.getAndIncrement()));
                kustoSinkTaskSpy.put(new ArrayList<>(records));
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignore) {
                }
            }
        };
        Future<?> runner = executor.submit(insertExec);
        Thread.sleep(500);
        stoppedObj.stopped = true;
        runner.cancel(true);
        int current = offset.get();
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topic1, new OffsetAndMetadata(current));

        Map<TopicPartition, OffsetAndMetadata> returnedOffsets = kustoSinkTaskSpy.preCommit(offsets);
        kustoSinkTaskSpy.close(tps);

        // Decrease one cause preCommit adds one
        assertEquals(returnedOffsets.get(topic1).offset() - 1, topicPartitionWriterSpy.lastCommittedOffset);
        Thread.sleep(500);
        // No ingestion occur even after waiting
        assertEquals(returnedOffsets.get(topic1).offset() - 1, topicPartitionWriterSpy.lastCommittedOffset);
    }

    static class Stopped {
        boolean stopped;
    }

    // ---------------------------------------------------------------------
    // getTopicsToIngestionProps — regression coverage for issue #174.
    // Verifies that the DataFormat ↔ IngestionMappingKind dispatch uses the
    // SDK enum as the single source of truth (instead of a hard-coded CSV
    // fallback) for every supported format.
    // ---------------------------------------------------------------------

    private static HashMap<String, String> baseConfigsWithTopicMapping(String topicMappingJson) {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        configs.put(KustoSinkConfig.KUSTO_TABLES_MAPPING_CONF, topicMappingJson);
        return configs;
    }

    /**
     * U1: For every supported DataFormat, with a non-blank mapping reference,
     * the resolved mapping kind must equal {@code dataFormat.getIngestionMappingKind()}.
     * This pins down the contract that broke for parquet/orc/w3clogfile (#174).
     */
    @ParameterizedTest(name = "[{index}] dataFormat={0}")
    @EnumSource(IngestionProperties.DataFormat.class)
    public void getTopicsToIngestionProps_withMapping_usesSdkMappingKind(IngestionProperties.DataFormat dataFormat) {
        String topicMapping = String.format(
                "[{'topic':'t1','db':'db1','table':'tbl1','format':'%s','mapping':'m1'}]",
                dataFormat.name().toLowerCase());
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        IngestionMapping.IngestionMappingKind expectedKind = dataFormat.getIngestionMappingKind();
        if (expectedKind == null) {
            // SDK contract: formats without a mapping kind cannot accept a named ingestion mapping.
            Assertions.assertThrows(ConfigException.class, () -> KustoSinkTask.getTopicsToIngestionProps(config));
            return;
        }

        Map<String, TopicIngestionProperties> result = KustoSinkTask.getTopicsToIngestionProps(config);
        IngestionProperties props = result.get("t1").ingestionProperties;
        IngestionProperties.DataFormat expectedDataFormat = dataFormat.isJsonFormat()
                ? IngestionProperties.DataFormat.MULTIJSON
                : dataFormat;
        assertEquals(expectedDataFormat, props.getDataFormat());
        assertEquals(expectedKind, props.getIngestionMapping().getIngestionMappingKind());
        assertEquals("m1", props.getIngestionMapping().getIngestionMappingReference());
    }

    /**
     * U2: For every supported DataFormat without a mapping reference, the data
     * format is still set correctly and no named ingestion mapping is attached.
     */
    @ParameterizedTest(name = "[{index}] dataFormat={0}")
    @EnumSource(IngestionProperties.DataFormat.class)
    public void getTopicsToIngestionProps_withoutMapping_setsDataFormatOnly(IngestionProperties.DataFormat dataFormat) {
        String topicMapping = String.format(
                "[{'topic':'t1','db':'db1','table':'tbl1','format':'%s'}]",
                dataFormat.name().toLowerCase());
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        Map<String, TopicIngestionProperties> result = KustoSinkTask.getTopicsToIngestionProps(config);
        IngestionProperties props = result.get("t1").ingestionProperties;
        IngestionProperties.DataFormat expectedDataFormat = dataFormat.isJsonFormat()
                ? IngestionProperties.DataFormat.MULTIJSON
                : dataFormat;
        assertEquals(expectedDataFormat, props.getDataFormat());
        assertTrue(props.getIngestionMapping() == null
                        || props.getIngestionMapping().getIngestionMappingReference() == null
                        || props.getIngestionMapping().getIngestionMappingReference().isEmpty(),
                "Expected no named ingestion mapping for format " + dataFormat);
    }

    /**
     * U3-U5: JSON, SINGLEJSON, MULTIJSON all collapse to dataFormat=MULTIJSON
     * (file writer constraint) while the mapping kind remains JSON.
     */
    @ParameterizedTest(name = "[{index}] format={0}")
    @ValueSource(strings = {"json", "JSON", "Json", "singlejson", "SINGLEJSON", "multijson", "MULTIJSON"})
    public void getTopicsToIngestionProps_jsonFamily_collapsesToMultijson(String format) {
        String topicMapping = String.format(
                "[{'topic':'t1','db':'db1','table':'tbl1','format':'%s','mapping':'m1'}]", format);
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        IngestionProperties props = KustoSinkTask.getTopicsToIngestionProps(config).get("t1").ingestionProperties;
        assertEquals(IngestionProperties.DataFormat.MULTIJSON, props.getDataFormat());
        assertEquals(IngestionMapping.IngestionMappingKind.JSON,
                props.getIngestionMapping().getIngestionMappingKind());
    }

    /**
     * U6: Case-insensitive format resolution — Parquet/PARQUET/parquet all map
     * to dataFormat=PARQUET and mapping kind=PARQUET. This was the #174 bug.
     */
    @ParameterizedTest(name = "[{index}] format={0}")
    @ValueSource(strings = {"parquet", "PARQUET", "Parquet", "pArQuEt"})
    public void getTopicsToIngestionProps_parquet_caseInsensitive(String format) {
        String topicMapping = String.format(
                "[{'topic':'t1','db':'db1','table':'tbl1','format':'%s','mapping':'m1'}]", format);
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        IngestionProperties props = KustoSinkTask.getTopicsToIngestionProps(config).get("t1").ingestionProperties;
        assertEquals(IngestionProperties.DataFormat.PARQUET, props.getDataFormat());
        assertEquals(IngestionMapping.IngestionMappingKind.PARQUET,
                props.getIngestionMapping().getIngestionMappingKind());
    }

    /**
     * U7: Invalid (typo'd) format must fail fast with a ConfigException that
     * lists the supported formats — replaces the prior silent CSV fallback.
     */
    @Test
    public void getTopicsToIngestionProps_invalidFormat_throwsConfigException() {
        String topicMapping = "[{'topic':'t1','db':'db1','table':'tbl1','format':'parqet','mapping':'m1'}]";
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        ConfigException ex = Assertions.assertThrows(ConfigException.class,
                () -> KustoSinkTask.getTopicsToIngestionProps(config));
        assertTrue(ex.getMessage().contains("parqet"),
                "Error message should mention the offending format token, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("PARQUET"),
                "Error message should list valid formats, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("t1"),
                "Error message should mention the topic, got: " + ex.getMessage());
    }

    /**
     * U8: Blank or whitespace-only format preserves the historical default
     * (CSV); when paired with a mapping reference, the mapping kind is CSV.
     */
    @ParameterizedTest(name = "[{index}] hasMapping={0}")
    @CsvSource({"true", "false"})
    public void getTopicsToIngestionProps_blankFormat_defaultsToCsv(boolean hasMapping) {
        String topicMapping = hasMapping
                ? "[{'topic':'t1','db':'db1','table':'tbl1','mapping':'m1'}]"
                : "[{'topic':'t1','db':'db1','table':'tbl1'}]";
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        IngestionProperties props = KustoSinkTask.getTopicsToIngestionProps(config).get("t1").ingestionProperties;
        assertEquals(IngestionProperties.DataFormat.CSV, props.getDataFormat());
        if (hasMapping) {
            assertEquals(IngestionMapping.IngestionMappingKind.CSV,
                    props.getIngestionMapping().getIngestionMappingKind());
        }
    }

    /**
     * U9: Multiple topic-to-table mappings in a single config are resolved
     * independently — parquet on one topic must not contaminate json on another.
     */
    @Test
    public void getTopicsToIngestionProps_multiTopic_resolvedIndependently() {
        String topicMapping = "["
                + "{'topic':'tp','db':'db1','table':'tparquet','format':'parquet','mapping':'pm'},"
                + "{'topic':'tj','db':'db1','table':'tjson','format':'json','mapping':'jm'},"
                + "{'topic':'tc','db':'db1','table':'tcsv','format':'csv','mapping':'cm'},"
                + "{'topic':'to','db':'db1','table':'torc','format':'orc','mapping':'om'}"
                + "]";
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        Map<String, TopicIngestionProperties> result = KustoSinkTask.getTopicsToIngestionProps(config);
        assertEquals(IngestionProperties.DataFormat.PARQUET, result.get("tp").ingestionProperties.getDataFormat());
        assertEquals(IngestionMapping.IngestionMappingKind.PARQUET,
                result.get("tp").ingestionProperties.getIngestionMapping().getIngestionMappingKind());
        assertEquals(IngestionProperties.DataFormat.MULTIJSON, result.get("tj").ingestionProperties.getDataFormat());
        assertEquals(IngestionMapping.IngestionMappingKind.JSON,
                result.get("tj").ingestionProperties.getIngestionMapping().getIngestionMappingKind());
        assertEquals(IngestionProperties.DataFormat.CSV, result.get("tc").ingestionProperties.getDataFormat());
        assertEquals(IngestionMapping.IngestionMappingKind.CSV,
                result.get("tc").ingestionProperties.getIngestionMapping().getIngestionMappingKind());
        assertEquals(IngestionProperties.DataFormat.ORC, result.get("to").ingestionProperties.getDataFormat());
        assertEquals(IngestionMapping.IngestionMappingKind.ORC,
                result.get("to").ingestionProperties.getIngestionMapping().getIngestionMappingKind());
    }

    /**
     * U10: The {@code streaming: true} flag must be propagated to the
     * resulting {@link TopicIngestionProperties}.
     */
    @ParameterizedTest(name = "[{index}] streaming={0}")
    @ValueSource(booleans = {true, false})
    public void getTopicsToIngestionProps_propagatesStreamingFlag(boolean streaming) {
        String topicMapping = String.format(
                "[{'topic':'t1','db':'db1','table':'tbl1','format':'csv','streaming':%s}]", streaming);
        KustoSinkConfig config = new KustoSinkConfig(baseConfigsWithTopicMapping(topicMapping));

        assertEquals(streaming, KustoSinkTask.getTopicsToIngestionProps(config).get("t1").streaming);
    }

    /**
     * U11: resolveDataFormat exposes the lookup helper directly — exercised
     * to lock down the trim+upper+lookup contract independently of the JSON
     * collapse and the surrounding config plumbing.
     */
    @Test
    public void resolveDataFormat_directContract() {
        assertEquals(IngestionProperties.DataFormat.PARQUET,
                KustoSinkTask.resolveDataFormat("parquet", "t1"));
        assertEquals(IngestionProperties.DataFormat.PARQUET,
                KustoSinkTask.resolveDataFormat("  PARQUET\t", "t1"));
        assertEquals(IngestionProperties.DataFormat.CSV,
                KustoSinkTask.resolveDataFormat("", "t1"));
        assertEquals(IngestionProperties.DataFormat.CSV,
                KustoSinkTask.resolveDataFormat(null, "t1"));
        assertEquals(IngestionProperties.DataFormat.CSV,
                KustoSinkTask.resolveDataFormat("   ", "t1"));
        Assertions.assertThrows(ConfigException.class,
                () -> KustoSinkTask.resolveDataFormat("notaformat", "t1"));
    }
}
