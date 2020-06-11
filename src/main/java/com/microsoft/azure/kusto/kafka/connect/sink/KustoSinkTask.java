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
import org.testng.util.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "committed" in the sense that we can advance the offset according to it.
 */
public class KustoSinkTask extends SinkTask {
    
  private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    static final int ROLE_INDEX = 0;
    static final int PRINCIPAL_DISPLAY_NAME_INDEX = 2;
    
    private final Set<TopicPartition> assignment;
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    private long maxFileSize;
    private long flushInterval;
    private String tempDir;
    private boolean commitImmediately;
    private int retiresCount;
    
    IngestClient kustoIngestClient;
    Map<TopicPartition, TopicPartitionWriter> writers;

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

            if (config.getAuthUsername() != null) {
                if (Strings.isNullOrEmpty(config.getAuthPassword())) {
                    throw new ConfigException("Kusto authentication missing Password.");
                }

                return IngestClientFactory.createClient(ConnectionStringBuilder.createWithAadUserCredentials(
                        config.getKustoUrl(),
                        config.getAuthUsername(),
                        config.getAuthPassword()
                ));
            }

            throw new ConnectException("Failed to initialize KustoIngestClient, please " +
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

            if (config.getAuthUsername() != null) {
                if (Strings.isNullOrEmpty(config.getAuthPassword())) {
                    throw new ConfigException("Kusto authentication missing Password.");
                }
                return ClientFactory.createClient(ConnectionStringBuilder.createWithAadUserCredentials(
                        engineClientURL,
                        config.getAuthUsername(),
                        config.getAuthPassword()
                ));
            }
            throw new ConnectException("Failed to initialize KustoEngineClient, please " +
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
                CompressionType compressionType = StringUtils.isBlank(mapping.optString("eventDataCompression")) ? null : CompressionType.valueOf(mapping.optString("eventDataCompression"));

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

    private void validateTableMappings(KustoSinkConfig config) {
        List<String> databaseTableErrorList = new ArrayList<>();
        List<String> accessErrorList = new ArrayList<>();
        try {
            Client engineClient = createKustoEngineClient(config);
            if (config.getTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getTopicToTableMapping());
                for (int i = 0; i < mappings.length(); i++) {
                    JSONObject mapping = mappings.getJSONObject(i);
                    validateTableAccess(config, engineClient, mapping, databaseTableErrorList, accessErrorList);
                }
            }
            String tableAccessErrorMessage = "";

            if(!databaseTableErrorList.isEmpty())
            {
                tableAccessErrorMessage = "\n\nUnable to access the following database:table\n" +
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

    /**
     *  This function validates whether the user has the read and write access to the intended table
     *  before starting to sink records into ADX.
     * @param config KustoSinkConfig class as defined by the user.
     * @param engineClient Client connection to run queries.
     * @param mapping JSON Object containing a Table mapping.
     */
    private static void validateTableAccess(KustoSinkConfig config, Client engineClient, JSONObject mapping, List<String> databaseTableErrorList, List<String> accessErrorList) throws JSONException {
        String getPrincipalsQuery = ".show table %s principals";
        String database = mapping.getString("db");
        String table = mapping.getString("table");
        try {
            Results rs = engineClient.execute(database, String.format(getPrincipalsQuery, table));
            String authenticateWith;
            if (config.getAuthAppid() != null) {
                authenticateWith = config.getAuthAppid();
            } else {
                authenticateWith=config.getAuthUsername();
            }
            boolean hasAccess = false;
            for (int i = 0; i<rs.getValues().size(); i++) {
                ArrayList<String> principal = rs.getValues().get(i);
                if (principal.get(PRINCIPAL_DISPLAY_NAME_INDEX).contains(authenticateWith)) {
                    if (principal.get(ROLE_INDEX).toLowerCase().contains("admin") ||
                            principal.get(ROLE_INDEX).toLowerCase().contains("ingestor")) {
                        hasAccess = true;
                        break;
                    }
                }
            }
            if (!hasAccess) {
                accessErrorList.add(String.format("%s:%s", database, table));
            }
            log.info("User has appropriate permissions to sink data into the Kusto table={}", table);
        } catch (DataClientException e) {
            throw new ConnectException("Unable to connect to ADX(Kusto) instance", e);
        } catch (DataServiceException e) {
            // Logging the error so that the trace is not lost.
            log.error("The table {} in database {} might not exist or access denied ", table, database, e);
            databaseTableErrorList.add(String.format("Database:%s Table:%s", database, table));
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
            log.debug(String.format("Open Kusto topic: '%s' with partition: '%s'", tp.topic(), tp.partition()));
            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, ingestionProps, tempDir, maxFileSize, flushInterval, commitImmediately, retiresCount);

                writer.open();
                writers.put(tp, writer);
            }
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        log.warn("KustoConnector got a request to close sink task");
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
    public void start(Map<String, String> props) {
        
        KustoSinkConfig config = new KustoSinkConfig(props);
        String url = config.getKustoUrl();
      
        validateTableMappings(config);
        
        topicsToIngestionProps = getTopicsToIngestionProps(config);
        
        // this should be read properly from settings
        kustoIngestClient = createKustoIngestClient(config);
        
        tempDir = config.getTempDirPath();
        maxFileSize = config.getFlushSizeBytes();
        flushInterval = config.getFlushInterval();
        commitImmediately = config.getKustoCommitImmediatly();
        retiresCount = config.getKustoRetriesCount();
        
        log.info(String.format("Started KustoSinkTask with target cluster: (%s), source topics: (%s)", 
            url, topicsToIngestionProps.keySet().toString()));
        open(context.assignment());
    }

    @Override
    public void stop() throws ConnectException {
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
        log.debug("put '"+ records.size() + "' num of records");
        int i = 0;
        SinkRecord lastRecord = null;
        for (SinkRecord record : records) {
            if (i == 0) {
                log.debug("First record is to topic:" + record.topic());
                log.debug("First record offset:" + record.kafkaOffset());
            }
            lastRecord = record;
            i++;
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
            log.debug("Last record was for topic:" + lastRecord.topic());
            log.debug("Last record had offset:" + lastRecord.kafkaOffset());
        }
    }

    // This is a neat trick, since our rolling files commit whenever they like, offsets may drift
    // from what kafka expects. So basically this is to re-sync topic-partition offsets with our sink.
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        log.info("preCommit called and sink is configured to " + (commitImmediately ? "flush and commit immediatly": "commit only successfully sent for ingestion offsets"));
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        if (commitImmediately) {
            CompletableFuture[] tasks = new CompletableFuture[assignment.size()];
            int i = 0;
            for (TopicPartition tp : assignment) {
                TopicPartitionWriter topicPartitionWriter = writers.get(tp);
                tasks[i] = (CompletableFuture.runAsync(() -> topicPartitionWriter.fileWriter.flushByTimeImpl()));
            }
            CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(tasks);
            try {
                voidCompletableFuture.get(4L, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.warn("failed to flush some of the partition writers in 4 seconds before comitting");
            }
        } else {
            for (TopicPartition tp : assignment) {

                Long offset = writers.get(tp).lastCommittedOffset;

                if (offset != null) {
                    log.debug("Forwarding to framework request to commit offset: {} for {} while the offset is {}", offset, tp, offsets.get(tp));
                    offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
                }
            }
        }

        return commitImmediately ? offsets : offsetsToCommit;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
        // do nothing , rolling files can handle writing
    }
}
