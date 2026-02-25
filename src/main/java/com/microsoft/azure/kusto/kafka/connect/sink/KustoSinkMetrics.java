package com.microsoft.azure.kusto.kafka.connect.sink;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX metrics for the Kusto Sink Connector.
 * Exposes counters for ingestion tracking via the MBean:
 * {@code com.microsoft.azure.kusto.kafka.connect.sink:type=KustoSinkMetrics}
 */
public class KustoSinkMetrics implements KustoSinkMetricsMXBean {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkMetrics.class);
    static final String MBEAN_NAME = "com.microsoft.azure.kusto.kafka.connect.sink:type=KustoSinkMetrics";

    private final AtomicLong recordsWritten = new AtomicLong();
    private final AtomicLong recordsFailed = new AtomicLong();
    private final AtomicLong ingestionAttempts = new AtomicLong();
    private final AtomicLong ingestionSuccesses = new AtomicLong();
    private final AtomicLong ingestionFailures = new AtomicLong();
    private final AtomicLong dlqRecordsSent = new AtomicLong();

    private ObjectName objectName;

    public KustoSinkMetrics() {
        register();
    }

    private void register() {
        try {
            objectName = new ObjectName(MBEAN_NAME);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(objectName)) {
                mbs.unregisterMBean(objectName);
            }
            mbs.registerMBean(this, objectName);
            log.info("Registered JMX MBean: {}", MBEAN_NAME);
        } catch (JMException e) {
            log.warn("Failed to register JMX MBean: {}", MBEAN_NAME, e);
        }
    }

    /**
     * Unregisters the MBean from JMX and resets all counters.
     */
    public void close() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (objectName != null && mbs.isRegistered(objectName)) {
                mbs.unregisterMBean(objectName);
                log.info("Unregistered JMX MBean: {}", MBEAN_NAME);
            }
        } catch (JMException e) {
            log.warn("Failed to unregister JMX MBean: {}", MBEAN_NAME, e);
        }
    }

    // --- Increment methods ---

    public void incrementRecordsWritten() {
        recordsWritten.incrementAndGet();
    }

    public void incrementRecordsFailed() {
        recordsFailed.incrementAndGet();
    }

    public void incrementIngestionAttempts() {
        ingestionAttempts.incrementAndGet();
    }

    public void incrementIngestionSuccesses() {
        ingestionSuccesses.incrementAndGet();
    }

    public void incrementIngestionFailures() {
        ingestionFailures.incrementAndGet();
    }

    public void incrementDlqRecordsSent() {
        dlqRecordsSent.incrementAndGet();
    }

    // --- MXBean attribute getters ---

    @Override
    public long getRecordsWritten() {
        return recordsWritten.get();
    }

    @Override
    public long getRecordsFailed() {
        return recordsFailed.get();
    }

    @Override
    public long getIngestionAttempts() {
        return ingestionAttempts.get();
    }

    @Override
    public long getIngestionSuccesses() {
        return ingestionSuccesses.get();
    }

    @Override
    public long getIngestionFailures() {
        return ingestionFailures.get();
    }

    @Override
    public long getDlqRecordsSent() {
        return dlqRecordsSent.get();
    }
}
