package com.microsoft.azure.kusto.kafka.connect.sink.format;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.connect.sink.SinkRecord;

public interface RecordWriter extends Closeable {
    /**
     * Write a record to storage.
     *
     * @param record the record to persist.
     */
    void write(SinkRecord record) throws IOException;

    /**
     * Close this writer.
     */
    void close();

    /**
     * Flush writer's data and commit the records in Kafka. Optionally, this operation might also
     * close the writer.
     */
    void commit();
}
