package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import org.apache.commons.lang3.StringUtils;
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
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    IngestClient kustoIngestClient;
    Map<TopicPartition, TopicPartitionWriter> writers;
    private long maxFileSize;
    private long flushInterval;
    private String tempDir;
    private KustoSinkConfig config;

    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    public static IngestClient createKustoIngestClient(KustoSinkConfig config) throws Exception {
        if (config.getKustoAuthAppid() != null) {
            if (config.getAuthAppkey() == null) {
                throw new ConfigException("Kusto authentication missing App Key.");
            }

            ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    config.getKustoUrl(),
                    config.getKustoAuthAppid(),
                    config.getAuthAppkey(),
                    config.getAuthAuthority()
            );
            return IngestClientFactory.createClient(kcsb);
        }

        if (config.getAuthUsername() != null) {
            if (config.getAuthPassword() == null) {
                throw new ConfigException("Kusto authentication missing Password.");
            }

            return IngestClientFactory.createClient(ConnectionStringBuilder.createWithAadUserCredentials(
                    config.getKustoUrl(),
                    config.getAuthUsername(),
                    config.getAuthPassword()
            ));
        }

        throw new ConfigException("Kusto authentication method must be provided.");
    }

    public static Map<String, TopicIngestionProperties> getTopicsToIngestionProps(KustoSinkConfig config) throws ConfigException {
        Map<String, TopicIngestionProperties> result = new HashMap<>();

        try {
            if (config.getTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getTopicToTableMapping());

                for (int i =0;i< mappings.length();i++) {

                    JSONObject mapping = mappings.getJSONObject(i);

                    try {
                        String db = mapping.getString("db");
                        String table = mapping.getString("table");

                        String format = mapping.optString("format");
                        CompressionType compressionType = StringUtils.isBlank(mapping.optString("eventDataCompression")) ? null : CompressionType.valueOf(mapping.optString("eventDataCompression"));

                        IngestionProperties props = new IngestionProperties(db, table);

                        if (format != null && !format.isEmpty()) {
                            if (format.equalsIgnoreCase("json") || format.equalsIgnoreCase("singlejson")  || format.equalsIgnoreCase("multijson")){
                                props.setDataFormat("multijson");
                            }
                            props.setDataFormat(format);
                        }

                        String mappingRef = mapping.optString("mapping");

                        if (mappingRef != null && !mappingRef.isEmpty()) {
                            if (format != null) {
                                if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.json.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Json);
                                } else if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.avro.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Avro);
                                } else if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.apacheavro.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.ApacheAvro);
                                } else if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.parquet.toString())) {
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Parquet);
                                } else if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.orc.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Orc);
                                } else {
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Csv);
                                }
                            }
                        }
                        TopicIngestionProperties topicIngestionProperties = new TopicIngestionProperties();
                        topicIngestionProperties.eventDataCompression = compressionType;
                        topicIngestionProperties.ingestionProperties = props;
                        result.put(mapping.getString("topic"), topicIngestionProperties);
                    } catch (Exception ex) {
                        throw new ConfigException("Malformed topics to kusto ingestion props mappings", ex);
                    }
                }

                return result;
            }
        }
        catch (Exception ex) {
            throw new ConfigException(String.format("Error trying to parse kusto ingestion props %s",ex.getMessage()));
        }

        throw new ConfigException("Malformed topics to kusto ingestion props mappings");
    }

    public TopicIngestionProperties getIngestionProps(String topic) {
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
            TopicIngestionProperties ingestionProps = getIngestionProps(tp.topic());
            log.debug(String.format("Open Kusto topic: '%s' with partition: '%s'", tp.topic(), tp.partition()));
            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, ingestionProps, tempDir, maxFileSize, flushInterval, config);

                writer.open();
                writers.put(tp, writer);
            }
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            try {
                writers.get(tp).close();
                writers.remove(tp);
                assignment.remove(tp);
            } catch (ConnectException e) {
                log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
            }
        }
    }


    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            config = new KustoSinkConfig(props);
            String url = config.getKustoUrl();

            topicsToIngestionProps = getTopicsToIngestionProps(config);
            // this should be read properly from settings
            kustoIngestClient = createKustoIngestClient(config);
            tempDir = config.getTempDirPath();
            maxFileSize = config.getFlushSizeBytes();
            flushInterval = config.getFlushInterval();
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
        log.warn("Stopping KustoSinkTask");
        for (TopicPartitionWriter writer : writers.values()) {
            writer.close();
        }
        try {
            kustoIngestClient.close();
        } catch (IOException e) {
            log.error("Error closing kusto client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        SinkRecord lastRecord = null;
        for (SinkRecord record : records) {
            log.debug("record to topic:" + record.topic());

            lastRecord = record;
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter writer = writers.get(tp);

            if (writer == null) {
                NotFoundException e = new NotFoundException(String.format("Received a record without a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
                log.error("Error putting records: ", e);
                throw e;
            }

            writer.writeRecord(record);
        }

        if (lastRecord != null) {
            log.debug("Last record offset:" + lastRecord.kafkaOffset());
        }
    }

    // This is a neat trick, since our rolling files commit whenever they like, offsets may drift
    // from what kafka expects. So basically this is to re-sync topic-partition offsets with our sink.
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignment) {

            Long offset = writers.get(tp).lastCommittedOffset;

            if (offset != null) {
                log.debug("Forwarding to framework request to commit offset: {} for {} while the offset is {}", offset, tp, offsets.get(tp));
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
