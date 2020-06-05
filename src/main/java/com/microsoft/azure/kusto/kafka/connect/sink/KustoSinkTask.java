package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
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
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    public static IngestClient createKustoIngestClient(KustoSinkConfig config) throws Exception {
        if (config.getKustoAuthAppid() != null) {
            if (config.getKustoAuthAppkey() == null) {
                throw new ConfigException("Kusto authentication missing App Key.");
            }

            ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    config.getKustoUrl(),
                    config.getKustoAuthAppid(),
                    config.getKustoAuthAppkey(),
                    config.getKustoAuthAuthority()
            );
            kcsb.setClientVersionForTracing(Version.CLIENT_NAME + ":" + Version.getVersion());

            return IngestClientFactory.createClient(kcsb);
        }

        if (config.getKustoAuthUsername() != null) {
            if (config.getKustoAuthPassword() == null) {
                throw new ConfigException("Kusto authentication missing Password.");
            }

            return IngestClientFactory.createClient(ConnectionStringBuilder.createWithAadUserCredentials(
                    config.getKustoUrl(),
                    config.getKustoAuthUsername(),
                    config.getKustoAuthPassword()
            ));
        }

        throw new ConfigException("Kusto authentication method must be provided.");
    }

    public static Map<String, TopicIngestionProperties> getTopicsToIngestionProps(KustoSinkConfig config) throws ConfigException {
        Map<String, TopicIngestionProperties> result = new HashMap<>();

        try {
            if (config.getKustoTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getKustoTopicToTableMapping());

                for (int i =0;i< mappings.length();i++) {

                    JSONObject mapping = mappings.getJSONObject(i);

                    try {
                        String db = mapping.getString("db");
                        String table = mapping.getString("table");

                        String format = mapping.optString("format");
                        CompressionType compressionType = StringUtils.isBlank(mapping.optString("eventDataCompression")) ? null : CompressionType.valueOf(mapping.optString("eventDataCompression"));

                        IngestionProperties props = new IngestionProperties(db, table);

                        if (format != null && !format.isEmpty()) {
                            if (format.equals("json") || format.equals("singlejson")){
                                props.setDataFormat("multijson");
                            }
                            props.setDataFormat(format);
                        }

                        String mappingRef = mapping.optString("mapping");

                        if (mappingRef != null && !mappingRef.isEmpty()) {
                            if (format != null) {
                                if (format.equals(IngestionProperties.DATA_FORMAT.json.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.json);
                                } else if (format.equals(IngestionProperties.DATA_FORMAT.avro.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.avro);
                                } else if (format.equals(IngestionProperties.DATA_FORMAT.parquet.toString())) {
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.parquet);
                                } else if (format.equals(IngestionProperties.DATA_FORMAT.orc.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.orc);
                                } else {
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.csv);
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

    private void validateTableMappings(KustoSinkConfig config) {
        try {
            if (config.getKustoTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getKustoTopicToTableMapping());
                for (int i = 0; i < mappings.length(); i++) {
                    JSONObject mapping = mappings.getJSONObject(i);
                    String db = mapping.getString("db");
                    String table = mapping.getString("table");
                    validateTableAccess(config,db,table);
                }
            }
        } catch (JSONException e) {
            throw new ConfigException(String.format("Error trying to parse kusto ingestion props %s", e.getMessage()));
        }
    }

    /**
     *  This function validates whether the user has the read and write access to the intended table
     *  before starting to sink records into ADX.
     * @param config KustoSinkConfig class as defined by the user.
     * @param database Name of the database the user needs to write records in.
     * @param table The table to sink records into.
     */
    private static void validateTableAccess(KustoSinkConfig config, String database, String table) {

        ConnectionStringBuilder engineCsb =
                ConnectionStringBuilder.createWithAadApplicationCredentials(
                        config.getKustoUrl(),
                        config.getKustoAuthAppid(),
                        config.getKustoAuthAppkey(),
                        config.getKustoAuthAuthority()
                );
        try {
            Client engineClient = ClientFactory.createClient(engineCsb);
            // To check whether the table and database exist and fetch the schema of the existing table.
            Results rs = engineClient.execute(database, table);
            List<String> randomValueList = new ArrayList<>();
            String ingestQuery = ".ingest inline into table %s <| %s";
            // Creating random value list to be inserted as a test record into the table.
            // TODO Add the other datatypes supported in Kusto.
            for (Entry entry : rs.getColumnNameToType().entrySet()) {
                switch (entry.getValue().toString().toLowerCase()) {
                    case "string":
                        randomValueList.add("TestValue");
                        break;

                    case "int32":
                        randomValueList.add("1");
                        break;
                    default:
                        break;
                }
            }
            String randomValues = String.join(",", randomValueList);
            // Adding a temporary record in order to check for write privileges.
            rs = engineClient.execute(database, String.format(ingestQuery, table, randomValues));
            // Extracting ExtentId to delete the temporary record.
            String extentId = rs.getValues().get(0).get(0);
            // Deleting the temporary record.
            engineClient.execute(database, String.format(".drop extent %s", extentId));
            log.info("User has appropriate permissions to sink data into the Kusto table={}", table);
        } catch (URISyntaxException e) {
            throw new ConnectException("Unable to connect to ADX(Kusto) instance due to invalid URL", e);
        } catch (DataClientException e) {
            throw new ConnectException("Unable to connect to ADX(Kusto) instance", e);
        } catch (DataServiceException e) {
            if (e.getCause().getMessage().contains("Database")) {
                throw new ConfigException(
                        String.format("Invalid or Denied Access to Database: %s", database), e);
            } else if (e.getCause().getMessage().contains("Table")) {
                if (config.getKustoAutoTableCreate()) {
                    // TODO add implementation for AutoTableCreate
                } else {
                    throw new ConfigException(
                            String.format("Invalid or Denied Access to Table: %s", table), e);
                }
            }
        }
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
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, ingestionProps, tempDir, maxFileSize, flushInterval);

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
            KustoSinkConfig config = new KustoSinkConfig(props);
            String url = config.getKustoUrl();
            validateTableMappings(config);
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
        for (SinkRecord record : records) {
            log.debug("record to topic:" + record.topic());

            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter writer = writers.get(tp);

            if (writer == null) {
                NotFoundException e = new NotFoundException(String.format("Received a record without a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
                log.error("Error putting records: ", e);
                throw e;
            }

            writer.writeRecord(record);
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
