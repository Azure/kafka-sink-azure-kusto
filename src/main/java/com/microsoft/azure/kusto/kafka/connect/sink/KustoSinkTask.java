package com.microsoft.azure.kusto.kafka.connect.sink;

import com.google.common.base.Strings;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;


/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "committed" in the sense that we can advance the offset according to it.
 */
public class KustoSinkTask extends SinkTask {
    
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    
    static final String FETCH_TABLE_QUERY = "%s|count";
    static final String FETCH_TABLE_MAPPING_QUERY = ".show table %s ingestion %s mapping '%s'";
    static final String FETCH_PRINCIPAL_ROLES_QUERY = ".show principal access with (principal = '%s', accesstype='ingest',database='%s',table='%s')";
    static final int INGESTION_ALLOWED_INDEX = 3;
    
    private final Set<TopicPartition> assignment;
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    private KustoSinkConfig config;
    IngestClient kustoIngestClient;
    Map<TopicPartition, TopicPartitionWriter> writers;
    private long maxFileSize;
    private long flushInterval;
    private String tempDir;
    private boolean isDlqEnabled;
    private String dlqTopicName;
    private Producer<byte[], byte[]> dlqProducer;

    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    public static IngestClient createKustoIngestClient(KustoSinkConfig config) {
        try {
            if (!Strings.isNullOrEmpty(config.getAuthAppid())) {
                if (Strings.isNullOrEmpty(config.getAuthAppkey())) {
                    throw new ConfigException("Kusto authentication missing App Key.");
                }

                ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                        config.getKustoUrl(),
                        config.getAuthAppid(),
                        config.getAuthAppkey(),
                        config.getAuthAuthority()
                );
                kcsb.setClientVersionForTracing(Version.CLIENT_NAME + ":" + Version.getVersion());

                return IngestClientFactory.createClient(kcsb);
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
            String engineClientURL = config.getKustoUrl().replace("https://ingest-", "https://");
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
  
            for (int i =0; i< mappings.length(); i++) {
  
                JSONObject mapping = mappings.getJSONObject(i);
  
                String db = mapping.getString("db");
                String table = mapping.getString("table");
  
                String format = mapping.optString("format");

                IngestionProperties props = new IngestionProperties(db, table);
  
                if (format != null && !format.isEmpty()) {
                    if (format.equals("json") || format.equals("singlejson") || format.equalsIgnoreCase("multijson")) {
                        props.setDataFormat("multijson");
                    }
                    props.setDataFormat(format);
                }
  
                if (format != null && !format.isEmpty()) {
                    if (format.equals("json") || format.equals("singlejson")){
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
                        }  else {
                            props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.Csv);
                        }
                    }
                }
                TopicIngestionProperties topicIngestionProperties = new TopicIngestionProperties();
                topicIngestionProperties.ingestionProperties = props;
                result.put(mapping.getString("topic"), topicIngestionProperties);
            }
            return result;
        }
        catch (Exception ex) {
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
                if(mappings.length() > 0) {
                    if(isIngestorRole(mappings.getJSONObject(0), engineClient)) {
                        for (int i = 0; i < mappings.length(); i++) {
                            JSONObject mapping = mappings.getJSONObject(i);
                            validateTableAccess(engineClient, mapping, config, databaseTableErrorList, accessErrorList);
                        }
                    }
                }
            }
            String tableAccessErrorMessage = "";

            if(!databaseTableErrorList.isEmpty())
            {
                tableAccessErrorMessage = "\n\nError occurred while trying to access the following database:table\n" +
                        String.join("\n",databaseTableErrorList);
            }
            if(!accessErrorList.isEmpty())
            {
                tableAccessErrorMessage = tableAccessErrorMessage + "\n\nUser does not have appropriate permissions " +
                        "to sink data into the Kusto database:table combination(s). " +
                        "Verify your Kusto principals and roles before proceeding for the following: \n " +
                        String.join("\n",accessErrorList);
            }

            if(!tableAccessErrorMessage.isEmpty()) {
                throw new ConnectException(tableAccessErrorMessage);
            }
        } catch (JSONException e) {
            throw new ConnectException("Failed to parse ``kusto.tables.topics.mapping`` configuration.", e);
        }
    }

    private boolean isIngestorRole(JSONObject testMapping, Client engineClient) throws JSONException {
        String database = testMapping.getString("db");
        String table = testMapping.getString("table");
        try {
            KustoOperationResult rs = engineClient.execute(database, String.format(FETCH_TABLE_QUERY, table));
        } catch(DataServiceException | DataClientException err){
            if(err.getCause().getMessage().contains("Forbidden:")){
                log.warn("User might have ingestor privileges, table validation will be skipped for all table mappings ");
                return false;
            }
        }
        return true;
    }

    /**
     *  This function validates whether the user has the read and write access to the intended table
     *  before starting to sink records into ADX.
     * @param engineClient Client connection to run queries.
     * @param mapping JSON Object containing a Table mapping.
     * @param config
     */
    private static void validateTableAccess(Client engineClient, JSONObject mapping, KustoSinkConfig config, List<String> databaseTableErrorList, List<String> accessErrorList) throws JSONException {
        
        String database = mapping.getString("db");
        String table = mapping.getString("table");
        String format = mapping.getString("format");
        String mappingName = mapping.getString("mapping");
        boolean hasAccess = false;
        try {
            try {
                KustoOperationResult rs = engineClient.execute(database, String.format(FETCH_TABLE_QUERY, table));
                if ((int) rs.getPrimaryResults().getData().get(0).get(0) >= 0) {
                    hasAccess = true;
                }

            } catch (DataServiceException e) {
                databaseTableErrorList.add(String.format("Database:%s Table:%s | table not found", database, table));
            }
            if(hasAccess) {
                try {
                    KustoOperationResult rp = engineClient.execute(database, String.format(FETCH_TABLE_MAPPING_QUERY, table, format, mappingName));
                    if (rp.getPrimaryResults().getData().get(0).get(0).toString().equals(mappingName)) {
                        hasAccess = true;
                    }
                } catch (DataServiceException e) {
                    hasAccess = false;
                    databaseTableErrorList.add(String.format("Database:%s Table:%s | %s mapping '%s' not found", database, table, format, mappingName));

                }
            }
            if(hasAccess) {
                try {
                    String authenticateWith = "aadapp=" + config.getAuthAppid();
                    KustoOperationResult rs = engineClient.execute(database, String.format(FETCH_PRINCIPAL_ROLES_QUERY, authenticateWith, database, table));
                    hasAccess = (boolean) rs.getPrimaryResults().getData().get(0).get(INGESTION_ALLOWED_INDEX);
                    if (hasAccess) {
                        log.info("User has appropriate permissions to sink data into the Kusto table={}", table);
                    } else {
                        accessErrorList.add(String.format("User does not have appropriate permissions " +
                                "to sink data into the Kusto database %s", database));
                    }
                } catch (DataServiceException e) {
                    // Logging the error so that the trace is not lost.
                    log.error("{}", e);
                    databaseTableErrorList.add(String.format("Database:%s Table:%s", database, table));
                }
            }
        } catch (DataClientException e) {
            throw new ConnectException("Unable to connect to ADX(Kusto) instance", e);
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
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped " +
                        "for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, ingestionProps, config, isDlqEnabled, dlqTopicName, dlqProducer);
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
                log.error("Error closing writer for {}. Error: {}", tp, e);
            }
        }
    }

    @Override
    public void start(Map<String, String> props) {

        config = new KustoSinkConfig(props);
        String url = config.getKustoUrl();
      
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
        kustoIngestClient = createKustoIngestClient(config);
        
        log.info(String.format("Started KustoSinkTask with target cluster: (%s), source topics: (%s)", 
            url, topicsToIngestionProps.keySet().toString()));
        // Adding this check to make code testable
        if(context!=null) {
            open(context.assignment());
        }
    }

    @Override
    public void stop() {
        log.warn("Stopping KustoSinkTask");
        for (TopicPartitionWriter writer : writers.values()) {
            writer.close();
        }
        try {
            if(kustoIngestClient != null) {
                kustoIngestClient.close();
            }
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
                NotFoundException e = new NotFoundException(String.format("Received a record without " +
                        "a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
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
            if(writers.get(tp) == null) {
                throw new ConnectException("Topic Partition not configured properly. " +
                        "verify your `topics` and `kusto.tables.topics.mapping` configurations");
            }

            Long lastCommittedOffset = writers.get(tp).lastCommittedOffset;

            if (lastCommittedOffset != null) {
                Long offset = lastCommittedOffset + 1L;
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
