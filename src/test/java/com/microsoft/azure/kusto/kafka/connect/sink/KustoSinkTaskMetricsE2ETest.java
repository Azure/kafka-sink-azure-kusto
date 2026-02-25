package com.microsoft.azure.kusto.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * End-to-end test that exercises the full KustoSinkTask lifecycle
 * (start → open → put → preCommit → stop) and verifies that JMX
 * metrics are registered, updated, and unregistered correctly.
 */
public class KustoSinkTaskMetricsE2ETest {

    private KustoSinkTask kustoSinkTask;
    private MBeanServer mbs;
    private ObjectName objectName;

    @BeforeEach
    public void setUp() throws Exception {
        mbs = ManagementFactory.getPlatformMBeanServer();
        objectName = new ObjectName(KustoSinkMetrics.MBEAN_NAME);
    }

    @AfterEach
    public void tearDown() {
        if (kustoSinkTask != null) {
            kustoSinkTask.stop();
        }
    }

    @Test
    public void testMetricsRegisteredAfterStart() throws Exception {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        kustoSinkTask = new KustoSinkTask();
        KustoSinkTask spy = spy(kustoSinkTask);
        doNothing().when(spy).validateTableMappings(Mockito.any());
        spy.start(configs);
        kustoSinkTask = spy;

        assertTrue(mbs.isRegistered(objectName),
                "JMX MBean should be registered after task start");
        assertEquals(0L, mbs.getAttribute(objectName, "RecordsWritten"));
        assertEquals(0L, mbs.getAttribute(objectName, "RecordsFailed"));
        assertEquals(0L, mbs.getAttribute(objectName, "IngestionAttempts"));
        assertEquals(0L, mbs.getAttribute(objectName, "IngestionSuccesses"));
        assertEquals(0L, mbs.getAttribute(objectName, "IngestionFailures"));
        assertEquals(0L, mbs.getAttribute(objectName, "DlqRecordsSent"));
    }

    @Test
    public void testMetricsUpdatedAfterPut() throws Exception {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        kustoSinkTask = new KustoSinkTask();
        KustoSinkTask spy = spy(kustoSinkTask);
        doNothing().when(spy).validateTableMappings(Mockito.any());
        spy.start(configs);
        kustoSinkTask = spy;

        ArrayList<TopicPartition> tps = new ArrayList<>();
        TopicPartition tp = new TopicPartition("topic1", 1);
        tps.add(tp);
        spy.open(tps);

        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null,
                "message1".getBytes(StandardCharsets.UTF_8), 1));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null,
                "message2".getBytes(StandardCharsets.UTF_8), 2));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null,
                "message3".getBytes(StandardCharsets.UTF_8), 3));
        spy.put(records);

        long recordsWritten = (long) mbs.getAttribute(objectName, "RecordsWritten");
        assertEquals(3L, recordsWritten,
                "RecordsWritten should reflect the number of records put");
    }

    @Test
    public void testMetricsUnregisteredAfterStop() {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        kustoSinkTask = new KustoSinkTask();
        KustoSinkTask spy = spy(kustoSinkTask);
        doNothing().when(spy).validateTableMappings(Mockito.any());
        spy.start(configs);

        assertTrue(mbs.isRegistered(objectName),
                "MBean should be registered after start");

        spy.stop();
        kustoSinkTask = null; // prevent double stop in tearDown

        assertFalse(mbs.isRegistered(objectName),
                "JMX MBean should be unregistered after task stop");
    }

    @Test
    public void testFullLifecycleMetrics() throws Exception {
        HashMap<String, String> configs = KustoSinkConnectorConfigTest.setupConfigs();
        kustoSinkTask = new KustoSinkTask();
        KustoSinkTask spy = spy(kustoSinkTask);
        doNothing().when(spy).validateTableMappings(Mockito.any());

        // Start
        spy.start(configs);
        kustoSinkTask = spy;
        assertTrue(mbs.isRegistered(objectName));

        // Open partitions
        ArrayList<TopicPartition> tps = new ArrayList<>();
        tps.add(new TopicPartition("topic1", 1));
        tps.add(new TopicPartition("topic2", 1));
        spy.open(tps);

        // Put records to topic1
        List<SinkRecord> records1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records1.add(new SinkRecord("topic1", 1, null, null, null,
                    ("msg" + i).getBytes(StandardCharsets.UTF_8), i));
        }
        spy.put(records1);

        // Put records to topic2
        List<SinkRecord> records2 = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            records2.add(new SinkRecord("topic2", 1, null, null, null,
                    ("msg" + i).getBytes(StandardCharsets.UTF_8), i));
        }
        spy.put(records2);

        // Verify combined metrics via JMX
        long totalRecordsWritten = (long) mbs.getAttribute(objectName, "RecordsWritten");
        assertEquals(8L, totalRecordsWritten,
                "Total records written across both topics should be 8");

        // Stop and verify cleanup
        spy.stop();
        kustoSinkTask = null;
        assertFalse(mbs.isRegistered(objectName),
                "MBean should be unregistered after stop");
    }
}
