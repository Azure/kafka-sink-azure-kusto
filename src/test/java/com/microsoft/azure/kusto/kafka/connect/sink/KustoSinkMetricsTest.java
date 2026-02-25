package com.microsoft.azure.kusto.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KustoSinkMetricsTest {

    private KustoSinkMetrics metrics;

    @BeforeEach
    public void setUp() {
        metrics = new KustoSinkMetrics();
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) {
            metrics.close();
        }
    }

    @Test
    public void testMBeanRegistered() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName(KustoSinkMetrics.MBEAN_NAME);
        assertTrue(mbs.isRegistered(objectName));
    }

    @Test
    public void testMBeanUnregisteredAfterClose() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName(KustoSinkMetrics.MBEAN_NAME);
        assertTrue(mbs.isRegistered(objectName));
        metrics.close();
        assertFalse(mbs.isRegistered(objectName));
        metrics = null;
    }

    @Test
    public void testInitialCountersAreZero() {
        assertEquals(0, metrics.getRecordsWritten());
        assertEquals(0, metrics.getRecordsFailed());
        assertEquals(0, metrics.getIngestionAttempts());
        assertEquals(0, metrics.getIngestionSuccesses());
        assertEquals(0, metrics.getIngestionFailures());
        assertEquals(0, metrics.getDlqRecordsSent());
    }

    @Test
    public void testIncrementRecordsWritten() {
        metrics.incrementRecordsWritten();
        metrics.incrementRecordsWritten();
        metrics.incrementRecordsWritten();
        assertEquals(3, metrics.getRecordsWritten());
    }

    @Test
    public void testIncrementRecordsFailed() {
        metrics.incrementRecordsFailed();
        assertEquals(1, metrics.getRecordsFailed());
    }

    @Test
    public void testIncrementIngestionAttempts() {
        metrics.incrementIngestionAttempts();
        metrics.incrementIngestionAttempts();
        assertEquals(2, metrics.getIngestionAttempts());
    }

    @Test
    public void testIncrementIngestionSuccesses() {
        metrics.incrementIngestionSuccesses();
        assertEquals(1, metrics.getIngestionSuccesses());
    }

    @Test
    public void testIncrementIngestionFailures() {
        metrics.incrementIngestionFailures();
        metrics.incrementIngestionFailures();
        assertEquals(2, metrics.getIngestionFailures());
    }

    @Test
    public void testIncrementDlqRecordsSent() {
        metrics.incrementDlqRecordsSent();
        assertEquals(1, metrics.getDlqRecordsSent());
    }

    @Test
    public void testMBeanAttributesViaJMX() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName(KustoSinkMetrics.MBEAN_NAME);

        metrics.incrementRecordsWritten();
        metrics.incrementIngestionSuccesses();
        metrics.incrementIngestionAttempts();

        assertEquals(1L, mbs.getAttribute(objectName, "RecordsWritten"));
        assertEquals(1L, mbs.getAttribute(objectName, "IngestionSuccesses"));
        assertEquals(1L, mbs.getAttribute(objectName, "IngestionAttempts"));
        assertEquals(0L, mbs.getAttribute(objectName, "RecordsFailed"));
        assertEquals(0L, mbs.getAttribute(objectName, "IngestionFailures"));
        assertEquals(0L, mbs.getAttribute(objectName, "DlqRecordsSent"));
    }

    @Test
    public void testConcurrentIncrements() throws InterruptedException {
        int threadCount = 10;
        int incrementsPerThread = 1000;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    metrics.incrementRecordsWritten();
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals((long) threadCount * incrementsPerThread, metrics.getRecordsWritten());
    }

    @Test
    public void testReRegistrationOnNewInstance() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName(KustoSinkMetrics.MBEAN_NAME);

        metrics.incrementRecordsWritten();
        assertEquals(1, metrics.getRecordsWritten());

        // Creating a new instance should re-register and reset
        KustoSinkMetrics newMetrics = new KustoSinkMetrics();
        assertTrue(mbs.isRegistered(objectName));
        assertEquals(0, newMetrics.getRecordsWritten());

        metrics.close();
        metrics = null;
        newMetrics.close();
    }
}
