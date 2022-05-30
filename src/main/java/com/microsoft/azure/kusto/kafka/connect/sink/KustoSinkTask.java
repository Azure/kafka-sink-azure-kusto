package com.microsoft.azure.kusto.kafka.connect.sink;

import com.google.common.base.Strings;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoDataException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
import java.util.*;


/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "committed" in the sense that we can advance the offset according to it.
 */
public class KustoSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);

    public static final String FETCH_TABLE_COMMAND = "%s | count";
    public static final String FETCH_TABLE_MAPPING_COMMAND = ".show table %s ingestion %s mapping '%s'";
    public static final String FETCH_PRINCIPAL_ROLES_COMMAND = ".show principal access with (principal = '%s', accesstype='ingest',database='%s',table='%s')";
    public static final String STREAMING_POLICY_SHOW_COMMAND = ".show %s %s policy streamingingestion";
    public static final int INGESTION_ALLOWED_INDEX = 3;
    public static final String MAPPING = "mapping";
    public static final String MAPPING_FORMAT = "format";
    public static final String MAPPING_TABLE = "table";
    public static final String DATABASE = "database";
    public static final String MAPPING_DB = "db";
    public static final String VALIDATION_OK = "OK";
    public static final String STREAMING = "streaming";

    private final Set<TopicPartition> assignment;
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    private KustoSinkConfig config;
    protected IngestClient kustoIngestClient;
    protected IngestClient streamingIngestClient;
    protected Map<TopicPartition, TopicPartitionWriter> writers;
    private boolean isDlqEnabled;
    private String dlqTopicName;
    private Producer<byte[], byte[]> dlqProducer;
    private static final ClientRequestProperties validateOnlyClientRequestProperties = new ClientRequestProperties();

    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
        validateOnlyClientRequestProperties.setOption("validate_permissions", true);
        // TODO we should check ingestor role differently
    }

    private static boolean isStreamingEnabled(KustoSinkConfig config) throws JSONException {
        JSONArray mappings = new JSONArray(config.getTopicToTableMapping());
        for (int i = 0; i < mappings.length(); i++) {
            JSONObject mapping = mappings.getJSONObject(i);
            if (mapping.optBoolean(STREAMING)) {
                return true;
            }
        }
        return false;
    }

    public void createKustoIngestClient(KustoSinkConfig config) {
        try {
            if (!Strings.isNullOrEmpty(config.getAuthAppid())) {
                if (Strings.isNullOrEmpty(config.getAuthAppkey())) {
                    throw new ConfigException("Kusto authentication missing App Key.");
                }

                ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                        config.getKustoIngestUrl(),
                        config.getAuthAppid(),
                        config.getAuthAppkey(),
                        config.getAuthAuthority()
                );
                kcsb.setClientVersionForTracing(Version.CLIENT_NAME + ":" + Version.getVersion());
                if (isStreamingEnabled(config)){
                    ConnectionStringBuilder engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                            config.getKustoEngineUrl(),
                            config.getAuthAppid(),
                            config.getAuthAppkey(),
                            config.getAuthAuthority()
                    );
                    streamingIngestClient = IngestClientFactory.createManagedStreamingIngestClient(kcsb, engineKcsb);
                }

                kustoIngestClient = IngestClientFactory.createClient(kcsb);
                return;
            }

            throw new ConfigException("Failed to initialize KustoIngestClient, please " +
                    "provide valid credentials. Either Kusto username and password or " +
                    "Kusto appId, appKey, and authority should be configured.");
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize KustoIngestClient", e);
        }
    }

    public static Client createKustoEngineClient(KustoSinkConfig config) {
        try {
            String engineClientURL = config.getKustoEngineUrl();
            if (!Strings.isNullOrEmpty(config.getAuthAppid())) {
                if (Strings.isNullOrEmpty(config.getAuthAppkey())) {
                    throw new ConfigException("Kusto authentication missing App Key.");
                }
                ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                        engineClientURL,
                        config.getAuthAppid(),
                        config.getAuthAppkey(),
                        config.getAuthAuthority()
                );
                kcsb.setClientVersionForTracing(Version.CLIENT_NAME + ":" + Version.getVersion());

                return ClientFactory.createClient(kcsb);
            }

            throw new ConfigException("Failed to initialize KustoEngineClient, please " +
                    "provide valid credentials. Either Kusto username and password or " +
                    "Kusto appId, appKey, and authority should be configured.");
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize KustoEngineClient", e);
        }
    }

    public static Map<String, TopicIngestionProperties> getTopicsToIngestionProps(KustoSinkConfig config) {
        Map<String, TopicIngestionProperties> result = new HashMap<>();

        try {
            JSONArray mappings = new JSONArray(config.getTopicToTableMapping());

            for (int i = 0; i < mappings.length(); i++) {
                JSONObject mapping = mappings.getJSONObject(i);

                String db = mapping.getString(MAPPING_DB);
                String table = mapping.getString(MAPPING_TABLE);

                String format = mapping.optString(MAPPING_FORMAT);
                boolean streaming = mapping.optBoolean(STREAMING);

                IngestionProperties props = new IngestionProperties(db, table);

                if (format != null && !format.isEmpty()) {
                    if (isDataFormatAnyTypeOfJson(format)) {
                        props.setDataFormat(IngestionProperties.DATA_FORMAT.multijson);
                    } else {
                        props.setDataFormat(format);
                    }
                }

                String mappingRef = mapping.optString(MAPPING);

                if (mappingRef != null && !mappingRef.isEmpty() && format != null) {
                    if (isDataFormatAnyTypeOfJson(format)) {
                        props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Json);
                    } else if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.avro.toString())) {
                        props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Avro);
                    } else if (format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.apacheavro.toString())) {
                        props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.ApacheAvro);
                    } else {
                        props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Csv);
                    }
                }
                TopicIngestionProperties topicIngestionProperties = new TopicIngestionProperties();
                topicIngestionProperties.ingestionProperties = props;
                topicIngestionProperties.streaming = streaming;
                result.put(mapping.getString("topic"), topicIngestionProperties);
            }
            return result;
        } catch (Exception ex) {
            throw new ConfigException("Error while parsing kusto ingestion properties.", ex);
        }
    }

    public TopicIngestionProperties getIngestionProps(String topic) {
        return topicsToIngestionProps.get(topic);
    }

    void validateTableMappings(KustoSinkConfig config) {
        List<String> databaseTableErrorList = new ArrayList<>();
        List<String> accessErrorList = new ArrayList<>();
        try {
            Client engineClient = createKustoEngineClient(config);
            if (config.getTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getTopicToTableMapping());
                if ((mappings.length() > 0) && (isIngestorRole(mappings.getJSONObject(0), engineClient))) {
                    for (int i = 0; i < mappings.length(); i++) {
                        JSONObject mapping = mappings.getJSONObject(i);
                        validateTableAccess(engineClient, mapping, config, databaseTableErrorList, accessErrorList);
                    }
                }
            }
            String tableAccessErrorMessage = "";

            if (!databaseTableErrorList.isEmpty()) {
                tableAccessErrorMessage = "\n\nError occurred while trying to access the following database:table\n" +
                        String.join("\n", databaseTableErrorList);
            }
            if (!accessErrorList.isEmpty()) {
                tableAccessErrorMessage = tableAccessErrorMessage + "\n\nUser does not have appropriate permissions " +
                        "to sink data into the Kusto database:table combination(s). " +
                        "Verify your Kusto principals and roles before proceeding for the following: \n " +
                        String.join("\n", accessErrorList);
            }

            if (!tableAccessErrorMessage.isEmpty()) {
                throw new ConnectException(tableAccessErrorMessage);
            }
        } catch (JSONException e) {
            throw new ConnectException("Failed to parse ``kusto.tables.topics.mapping`` configuration.", e);
        }
    }

    private boolean isIngestorRole(JSONObject testMapping, Client engineClient) throws JSONException {
        String database = testMapping.getString(MAPPING_DB);
        String table = testMapping.getString(MAPPING_TABLE);
        try {
            engineClient.execute(database, String.format(FETCH_TABLE_COMMAND, table), validateOnlyClientRequestProperties);
        } catch (DataServiceException | DataClientException err) {
            if (err.getCause().getMessage().contains("Forbidden:")) {
                log.warn("User might have ingestor privileges, table validation will be skipped for all table mappings ");
                return false;
            }
        }
        return true;
    }

    private static boolean isDataFormatAnyTypeOfJson(String format) {
        return format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.json.name())
                || format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.singlejson.name())
                || format.equalsIgnoreCase(IngestionProperties.DATA_FORMAT.multijson.name());
    }

    /**
     * This function validates whether the user has the read and write access to the intended table
     * before starting to sink records into ADX.
     *
     * @param engineClient Client connection to run queries.
     * @param mapping      JSON Object containing a Table mapping.
     * @param config       Kusto Sink configuration
     */
    private static void validateTableAccess(Client engineClient, JSONObject mapping, KustoSinkConfig config, List<String> databaseTableErrorList, List<String> accessErrorList) throws JSONException {
        String database = mapping.getString(MAPPING_DB);
        String table = mapping.getString(MAPPING_TABLE);
        String format = mapping.getString(MAPPING_FORMAT);
        String mappingName = mapping.getString(MAPPING);
        boolean streamingEnabled = mapping.optBoolean(STREAMING);
        if (isDataFormatAnyTypeOfJson(format)) {
            format = IngestionProperties.DATA_FORMAT.json.name();
        }
        boolean hasAccess = false;
        boolean shouldCheckStreaming = streamingEnabled;

        try {
            if (shouldCheckStreaming && isStreamingPolicyEnabled(DATABASE, database, engineClient, database)){
                shouldCheckStreaming = false;
            }
            try {
                KustoOperationResult rs = engineClient.execute(database, String.format(FETCH_TABLE_COMMAND, table), validateOnlyClientRequestProperties);
                if (VALIDATION_OK.equals(rs.getPrimaryResults().getData().get(0).get(0))) {
                    hasAccess = true;
                }
            } catch (DataServiceException e) {
                databaseTableErrorList.add(String.format("Couldn't validate access to Database '%s' Table '%s', with exception '%s'", database, table, ExceptionUtils.getStackTrace(e)));
            }
            if (hasAccess) {
                try {
                    engineClient.execute(database, String.format(FETCH_TABLE_MAPPING_COMMAND, table, format, mappingName));
                } catch (DataServiceException e) {
                    hasAccess = false;
                    databaseTableErrorList.add(String.format("Database:%s Table:%s | %s mapping '%s' not found, with exception '%s'", database, table, format, mappingName, ExceptionUtils.getStackTrace(e)));
                }
            }
            if (hasAccess) {
                String authenticateWith = String.format("aadapp=%s;%s", config.getAuthAppid(), config.getAuthAuthority());
                String query = String.format(FETCH_PRINCIPAL_ROLES_COMMAND, authenticateWith, database, table);
                try {
                    KustoOperationResult rs = engineClient.execute(database, query);
                    hasAccess = (boolean) rs.getPrimaryResults().getData().get(0).get(INGESTION_ALLOWED_INDEX);
                    if (hasAccess) {
                                log.info("User has appropriate permissions to sink data into the Kusto table={}", table);
                    } else {
                        accessErrorList.add(String.format("User does not have appropriate permissions " +
                                "to sink data into the Kusto database %s", database));
                    }
                } catch (DataServiceException e) {
                    // Logging the error so that the trace is not lost.
                    if (!e.getCause().toString().contains("Forbidden")){
                        databaseTableErrorList.add(String.format("Fetching principal roles using query '%s' resulted in exception '%s'", query, ExceptionUtils.getStackTrace(e)));
                    } else {
                        log.warn("Failed to check permissions with query '{}', will continue the run as the principal might still be able to ingest", query, e);
                    }
                }
            }
            if (hasAccess && shouldCheckStreaming && !isStreamingPolicyEnabled(MAPPING_TABLE, table, engineClient, database)) {
                databaseTableErrorList.add(String.format("Ingestion is configured as streaming, but a streaming" +
                  " ingestion policy was not found on either database '%s' or table '%s'", database, table));
            }

        } catch (KustoDataException e) {
            throw new ConnectException("Unable to connect to ADX(Kusto) instance", e);
        }
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        assignment.addAll(partitions);
        for (TopicPartition tp : assignment) {
            TopicIngestionProperties ingestionProps = getIngestionProps(tp.topic());
            log.debug("Open Kusto topic: '{}' with partition: '{}'", tp.topic(), tp.partition());
            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped " +
                        "for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                IngestClient client = ingestionProps.streaming ? streamingIngestClient : kustoIngestClient;
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, client, ingestionProps, config, isDlqEnabled, dlqTopicName, dlqProducer);
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
                log.error("Error closing writer for {}.", tp, e);
            }
        }
    }

    @Override
    public void start(Map<String, String> props) {
        config = new KustoSinkConfig(props);
        String url = config.getKustoIngestUrl();

        validateTableMappings(config);
        if (config.isDlqEnabled()) {
            isDlqEnabled = true;
            dlqTopicName = config.getDlqTopicName();
            Properties properties = config.getDlqProps();
            log.info("Initializing miscellaneous dead-letter queue producer with the following properties: {}", properties.keySet());
            try {
                dlqProducer = new KafkaProducer<>(properties);
            } catch (Exception e) {
                throw new ConnectException("Failed to initialize producer for miscellaneous dead-letter queue", e);
            }

        } else {
            dlqProducer = null;
            isDlqEnabled = false;
            dlqTopicName = null;
        }

        topicsToIngestionProps = getTopicsToIngestionProps(config);

        // this should be read properly from settings
        createKustoIngestClient(config);

        log.info("Started KustoSinkTask with target cluster: ({}), source topics: ({})", url, topicsToIngestionProps.keySet());
        // Adding this check to make code testable
        if (context != null) {
            open(context.assignment());
        }
    }

    private static boolean isStreamingPolicyEnabled (
            String entityType, String entityName, Client engineClient, String database
    ) throws DataClientException, DataServiceException {
        KustoResultSetTable res = engineClient.execute(database, String.format(STREAMING_POLICY_SHOW_COMMAND, entityType, entityName)).getPrimaryResults();
        res.next();
        return res.getString("Policy") != null;
    }

    @Override
    public void stop() {
        log.warn("Stopping KustoSinkTask");
        // First stop so that no more ingestions trigger from timer flushes
        for (TopicPartitionWriter writer : writers.values()) {
            writer.stop();
        }

        for (TopicPartitionWriter writer : writers.values()) {
            writer.close();
        }
        try {
            if (kustoIngestClient != null) {
                kustoIngestClient.close();
            }
        } catch (IOException e) {
            log.error("Error closing kusto client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        SinkRecord lastRecord = null;
        for (SinkRecord sinkRecord : records) {
            log.debug("Record to topic: {}", sinkRecord.topic());

            lastRecord = sinkRecord;
            TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
            TopicPartitionWriter writer = writers.get(tp);

            if (writer == null) {
                NotFoundException e = new NotFoundException(String.format("Received a record without " +
                        "a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
                log.error("Error putting records: ", e);
                throw e;
            }

            writer.writeRecord(sinkRecord);
        }

        if (lastRecord != null) {
            log.debug("Last record offset: {}", lastRecord.kafkaOffset());
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
            if (writers.get(tp) == null) {
                throw new ConnectException("Topic Partition not configured properly. " +
                        "verify your `topics` and `kusto.tables.topics.mapping` configurations");
            }

            Long lastCommittedOffset = writers.get(tp).lastCommittedOffset;

            if (lastCommittedOffset != null) {
                long offset = lastCommittedOffset + 1L;
                log.debug("Forwarding to framework request to commit offset: {} for {} while the offset is {}", offset, tp, offsets.get(tp));
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }

        return offsetsToCommit;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // do nothing , rolling files can handle writing
    }
}