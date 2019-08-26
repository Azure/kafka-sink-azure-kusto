package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "commited" in the sense that we can advance the offset according to it.
 */
public class KustoSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private final Set<TopicPartition> assignment;
    IngestClient kustoIngestClient;
    Map<TopicPartition, TopicPartitionWriter> writers;
    private Map<String, IngestionProperties> topicsToIngestionProps;
    private long maxFileSize;
    private long flushInterval;
    private String tempDir;


    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    public static IngestClient createKustoIngestClient(KustoSinkConfig config) throws Exception {
        if (config.getKustoAuthAppid() != null) {
            if (config.getKustoAuthAppkey() == null) {
                throw new ConfigException("Kusto authentication missing App Key.");
            }

            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(
                    config.getKustoUrl(),
                    config.getKustoAuthAppid(),
                    config.getKustoAuthAppkey(),
                    config.getKustoAuthAuthority()
            );

            return IngestClientFactory.createClient(kcsb);
        }

        if (config.getKustoAuthUsername() != null) {
            if (config.getKustoAuthPassword() == null) {
                throw new ConfigException("Kusto authentication missing Password.");
            }

            return IngestClientFactory.createClient(KustoConnectionStringBuilder.createWithAadUserCredentials(
                    config.getKustoUrl(),
                    config.getKustoAuthUsername(),
                    config.getKustoAuthPassword()
            ));
        }

        throw new ConfigException("Kusto authentication method must be provided.");
    }

    public static Map<String, IngestionProperties> getTopicsToIngestionProps(KustoSinkConfig config) throws ConfigException {
        Map<String, IngestionProperties> result = new HashMap<>();

        try {
            if (config.getKustoTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getKustoTopicToTableMapping());

                for (int i = 0; i < mappings.length(); i++) {

                    JSONObject mapping = mappings.getJSONObject(i);

                    try {
                        String db = mapping.getString("db");
                        String table = mapping.getString("table");

                        String format = mapping.optString("format");
                        IngestionProperties props = new IngestionProperties(db, table);

                        if (format != null && !format.isEmpty()) {
                            props.setDataFormat(format);
                        }

                        String mappingRef = mapping.optString("mapping");

                        if (mappingRef != null && !mappingRef.isEmpty()) {
                            if (format != null && format.equals("json")) {
                                props.setJsonMappingName(mappingRef);
                            } else {
                                props.setCsvMappingName(mappingRef);
                            }
                        }

                        result.put(mapping.getString("topic"), props);
                    } catch (Exception ex) {
                        throw new ConfigException("Malformed topics to kusto ingestion props mappings");
                    }
                }

                return result;
            }
        } catch (Exception ex) {
            throw new ConfigException(String.format("Error trying to parse kusto ingestion props %s", ex.getMessage()));
        }

        throw new ConfigException("Malformed topics to kusto ingestion props mappings");
    }

    public IngestionProperties getIngestionProps(String topic) {
        return topicsToIngestionProps.get(topic);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) throws ConnectException {
        assignment.addAll(partitions);

        for (TopicPartition tp : assignment) {
            IngestionProperties ingestionProps = getIngestionProps(tp.topic());

            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, ingestionProps, tempDir, maxFileSize, flushInterval);

                writer.open();
                writers.put(tp, writer);
            }
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : assignment) {
            try {
                writers.get(tp).close();
            } catch (ConnectException | IOException e) {
                log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
            }
        }

        writers.clear();
        assignment.clear();
    }


    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            KustoSinkConfig config = new KustoSinkConfig(props);
            String url = config.getKustoUrl();

            topicsToIngestionProps = getTopicsToIngestionProps(config);
            // this should be read properly from settings
            kustoIngestClient = createKustoIngestClient(config);
            tempDir = config.getKustoSinkTempDir();
            maxFileSize = config.getKustoFlushSize();
            flushInterval = config.getKustoFlushIntervalMS();

            log.info(String.format("Kafka Kusto Sink started. target cluster: (%s), source topics: (%s)", url, topicsToIngestionProps.keySet().toString()));
            open(context.assignment());

        } catch (ConfigException ex) {
            throw new ConnectException(String.format("Kusto Connector failed to start due to configuration error. %s", ex.getMessage()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() throws ConnectException {
        for (TopicPartitionWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("Error closing writer for {}. Error: {}", writer, e.getMessage());
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        for (SinkRecord record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter writer = writers.get(tp);

            if (writer == null) {
                throw new NotFoundException(String.format("Received a record without a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
            }

            writer.writeRecord(record);
        }
    }

    // this is a neat trick, since our rolling files commit whenever they like, offsets may drift
    // from what kafka expects. so basically this is to re-sync topic-partition offsets with our sink.
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignment) {

            Long offset = writers.get(tp).lastCommittedOffset;

            if (offset != null) {
                log.debug("Forwarding to framework request to commit offset: {} for {}", offset, tp);
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }

        return offsetsToCommit;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
        // do nothing , rolling files can handle writing
    }
}
