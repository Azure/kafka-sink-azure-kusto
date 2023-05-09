package com.microsoft.azure.kusto.kafka.connect.sink;

import java.util.Objects;

import org.apache.kafka.common.config.ConfigException;

public class TopicToTableMapping {
    private String mapping;
    private String format;
    private String table;
    private String db;
    private String topic;
    private boolean streaming;

    public TopicToTableMapping() {
    }

    public TopicToTableMapping(String mapping, String format, String table, String db, String topic, boolean streaming) {
        this.mapping = mapping;
        this.format = format;
        this.table = table;
        this.db = db;
        this.topic = topic;
        this.streaming = streaming;
    }

    public String getMapping() {
        return mapping;
    }

    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public void setStreaming(boolean streaming) {
        this.streaming = streaming;
    }

    void validate() {
        if (null == db || db.isEmpty()) {
            throw new ConfigException("'db' must be provided for each mapping");
        }

        if (null == table || table.isEmpty()) {
            throw new ConfigException("'table' must be provided for each mapping");
        }

        if (null == topic || topic.isEmpty()) {
            throw new ConfigException("'topic' must be provided for each mapping");
        }
    }

    @Override
    public String toString() {
        return "KustoTableMapping{" +
                "mapping='" + mapping + '\'' +
                ", format='" + format + '\'' +
                ", table='" + table + '\'' +
                ", db='" + db + '\'' +
                ", topic='" + topic + '\'' +
                ", streaming=" + streaming +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TopicToTableMapping that = (TopicToTableMapping) o;
        return streaming == that.streaming && Objects.equals(mapping, that.mapping) && Objects.equals(format, that.format) && Objects.equals(table, that.table)
                && Objects.equals(db, that.db) && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapping, format, table, db, topic, streaming);
    }
}
