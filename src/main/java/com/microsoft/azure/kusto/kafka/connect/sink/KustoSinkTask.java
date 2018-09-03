package com.microsoft.azure.kusto.kafka.connect.sink;

//import com.microsoft.azure.kusto.kafka.connect.source.JsonSerialization
// FIXME: need to consume this package via maven once setup properly
//import com.microsoft.azure.sdk.kusto.ingest.KustoIngestClient;

import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoIngestClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "commited" in the sense that we can advance the offset according to it.
 */
public class KustoSinkTask extends SinkTask {
    static final String TOPICS_WILDCARD = "*";
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private final Set<TopicPartition> assignment;
    Map<String, String> topicsToTables;
    KustoIngestClient kustoIngestClient;
    String databaseName;
    Map<TopicPartition, TopicPartitionWriter> writers;
    private Long maxFileSize;
    private String tempDir;


    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    public static KustoIngestClient createKustoIngestClient(KustoSinkConfig config) throws Exception {
        if (config.getKustoAuthAppid() != null) {
            if (config.getKustoAuthAppkey() == null) {
                throw new ConfigException("Kusto authentication missing App Key.");
            }
            return new KustoIngestClient(KustoConnectionStringBuilder.createWithAadApplicationCredentials(
                    config.getKustoUrl(),
                    //todo: should replace with proper initialization
                    config.getKustoAuthAppid(),
                    config.getKustoAuthAppkey(),
                    config.getKustoAuthAuthority()
            ));
        }

        if (config.getKustoAuthUsername() != null) {
            if (config.getKustoAuthPassword() == null) {
                throw new ConfigException("Kusto authentication missing Password.");
            }

            return new KustoIngestClient(KustoConnectionStringBuilder.createWithAadUserCredentials(
                    config.getKustoUrl(),
                    //todo: should replace with proper initialization
                    config.getKustoAuthUsername(),
                    config.getKustoAuthPassword()
            ));
        }

        throw new ConfigException("Kusto authentication method must be provided.");
    }

    public static Map<String, String> getTopicsToTables(KustoSinkConfig config) {
        Map<String, String> result = new HashMap<>();

        if (config.getKustoTable() != null) {
            result.put(TOPICS_WILDCARD, config.getKustoTable());
            return result;
        }

        if (config.getKustoTopicToTableMapping() != null) {
            String[] mappings = config.getKustoTopicToTableMapping().split(";");

            for (String mapping : mappings) {
                String[] kvp = mapping.split(":");
                if (kvp.length != 2) {
                    throw new ConfigException("Provided table mapping is malformed. please make sure table mapping is of 'topicName:tableName;' format.");
                }

                result.put(kvp[0], kvp[1]);
            }

            return result;
        }

        throw new ConfigException("Kusto table mapping must be provided.");
    }

    public String getTable(String topic) {
        if (topicsToTables.containsKey(TOPICS_WILDCARD)) {
            return topicsToTables.get(TOPICS_WILDCARD);
        }

        return topicsToTables.get(topic);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) throws ConnectException {
        assignment.addAll(partitions);

        for (TopicPartition tp : assignment) {
            String table = getTable(tp.topic());

            if (table == null) {
                throw new ConnectException(String.format("Kusto Sink has no table mapped for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, databaseName, table, tempDir, maxFileSize);

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
            } catch (ConnectException e) {
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

            databaseName = config.getKustoDb();

            topicsToTables = getTopicsToTables(config);
            // this should be read properly from settings
            kustoIngestClient = createKustoIngestClient(config);
            tempDir = config.getKustoSinkTempDir();
            maxFileSize = config.getKustoFlushSize();

            open(context.assignment());

        } catch (ConfigException ex) {
            throw new ConnectException(String.format("Kusto Connector failed to start due to configuration error. %s", ex.getMessage()), ex);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() throws ConnectException {
        for (TopicPartitionWriter writer : writers.values()) {
            writer.close();
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
    // from what kafka expects. so basically this is to re-sync topic-partition offests with our sink.
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignment) {

            Long offset = writers.get(tp).lastCommitedOffset;

            if (offset != null) {
                log.trace("Forwarding to framework request to commit offset: {} for {}", offset, tp);
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
