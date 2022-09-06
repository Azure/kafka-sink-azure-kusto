package com.microsoft.azure.kusto.kafka.connect.sink;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KustoSinkConfig extends AbstractConfig {
    // TODO: this might need to be per kusto cluster...
    static final String KUSTO_INGEST_URL_CONF = "kusto.ingestion.url";
    static final String KUSTO_ENGINE_URL_CONF = "kusto.query.url";
    static final String KUSTO_AUTH_APPID_CONF = "aad.auth.appid";
    static final String KUSTO_AUTH_APPKEY_CONF = "aad.auth.appkey";
    static final String KUSTO_AUTH_AUTHORITY_CONF = "aad.auth.authority";
    static final String KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_CONF = "aad.auth.managedidentity.enabled";
    static final String KUSTO_TABLES_MAPPING_CONF = "kusto.tables.topics.mapping";
    static final String KUSTO_SINK_TEMP_DIR_CONF = "tempdir.path";
    static final String KUSTO_SINK_FLUSH_SIZE_BYTES_CONF = "flush.size.bytes";
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS_CONF = "flush.interval.ms";
    static final String KUSTO_BEHAVIOR_ON_ERROR_CONF = "behavior.on.error";
    static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF = "misc.deadletterqueue.bootstrap.servers";
    static final String KUSTO_DLQ_TOPIC_NAME_CONF = "misc.deadletterqueue.topic.name";
    static final String KUSTO_SINK_MAX_RETRY_TIME_MS_CONF = "errors.retry.max.time.ms";
    static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF = "errors.retry.backoff.time.ms";
    static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION = "kusto.validation.table.enable";
    private static final Logger log = LoggerFactory.getLogger(KustoSinkConfig.class);
    private static final String DLQ_PROPS_PREFIX = "misc.deadletterqueue.";
    private static final String KUSTO_INGEST_URL_DOC = "Kusto ingestion endpoint URL.";
    private static final String KUSTO_INGEST_URL_DISPLAY = "Kusto cluster ingestion URL";
    private static final String KUSTO_ENGINE_URL_DOC = "Kusto query endpoint URL.";
    private static final String KUSTO_ENGINE_URL_DISPLAY = "Kusto cluster query URL";
    private static final String KUSTO_AUTH_APPID_DOC = "Application Id for Azure Active Directory authentication.";
    private static final String KUSTO_AUTH_APPID_DISPLAY = "Kusto Auth AppID";
    private static final String KUSTO_AUTH_APPKEY_DOC = "Application Key for Azure Active Directory authentication.";
    private static final String KUSTO_AUTH_APPKEY_DISPLAY = "Kusto Auth AppKey";
    private static final String KUSTO_AUTH_AUTHORITY_DOC = "Azure Active Directory tenant.";
    private static final String KUSTO_AUTH_AUTHORITY_DISPLAY = "Kusto Auth Authority";
    private static final String KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_DOC = "If true, managed identity are used to authenticate against Azure Active Directory.";
    private static final String KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_DISPLAY = "Kusto Auth Managed Identity Enabled";
    private static final String KUSTO_TABLES_MAPPING_DOC = "A JSON array mapping ingestion from topic to table, e.g: "
            + "[{'topic1':'t1','db':'kustoDb', 'table': 'table1', 'format': 'csv', 'mapping': 'csvMapping', 'streaming': 'false'}..].\n"
            + "Streaming is optional, defaults to false. Mind usage and cogs of streaming ingestion, read here: https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming.\n"
            + "Note: If the streaming ingestion fails transiently,"
            + " queued ingest would apply for this specific batch ingestion. Batching latency is configured regularly via"
            + "ingestion batching policy";
    private static final String KUSTO_TABLES_MAPPING_DISPLAY = "Kusto Table Topics Mapping";
    private static final String KUSTO_SINK_TEMP_DIR_DOC = "Temp dir that will be used by kusto sink to buffer records. "
            + "defaults to system temp dir.";
    private static final String KUSTO_SINK_TEMP_DIR_DISPLAY = "Temporary Directory";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DOC = "Kusto sink max buffer size (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DISPLAY = "Maximum Flush Size";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DOC = "Kusto sink max staleness in milliseconds (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY = "Maximum Flush Interval";
    private static final String KUSTO_BEHAVIOR_ON_ERROR_DOC = "Behavior on error setting for "
            + "ingestion of records into Kusto table. "
            + "Must be configured to one of the following:\n"

            + "``fail``\n"
            + "    Stops the connector when an error occurs "
            + "while processing records or ingesting records in Kusto table.\n"

            + "``ignore``\n"
            + "    Continues to process next set of records "
            + "when error occurs while processing records or ingesting records in Kusto table.\n"

            + "``log``\n"
            + "    Logs the error message and continues to process subsequent records when an error occurs "
            + "while processing records or ingesting records in Kusto table, available in connect logs.";
    private static final String KUSTO_BEHAVIOR_ON_ERROR_DISPLAY = "Behavior On Error";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC = "Configure this list to Kafka broker's address(es) "
            + "to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster. "
            + "This list should be in the form host-1:port-1,host-2:port-2,…host-n:port-n.";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DISPLAY = "Miscellaneous Dead-Letter Queue Bootstrap Servers";
    private static final String KUSTO_DLQ_TOPIC_NAME_DOC = "Set this to the Kafka topic's name "
            + "to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster.";
    private static final String KUSTO_DLQ_TOPIC_NAME_DISPLAY = "Miscellaneous Dead-Letter Queue Topic Name";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DOC = "Maximum time up to which the Connector "
            + "should retry writing records to Kusto table in case of failures.";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DISPLAY = "Errors Maximum Retry Time";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC = "BackOff time between retry attempts "
            + "the Connector makes to ingest records into Kusto table.";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DISPLAY = "Errors Retry BackOff Time";
    private static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION_DOC = "Enable table access validation at task start.";
    private static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION_DISPLAY = "Enable table validation";

    public KustoSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public KustoSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
        ConfigDef result = new ConfigDef();

        defineConnectionConfigs(result);
        defineWriteConfigs(result);
        defineErrorHandlingAndRetriesConfigs(result);

        return result;
    }

    private static void defineErrorHandlingAndRetriesConfigs(ConfigDef result) {
        final String errorAndRetriesGroupName = "Error Handling and Retries";
        int errorAndRetriesGroupOrder = 0;

        result
                .define(
                        KUSTO_BEHAVIOR_ON_ERROR_CONF,
                        Type.STRING,
                        BehaviorOnError.FAIL.name(),
                        ConfigDef.ValidString.in(
                                BehaviorOnError.FAIL.name(), BehaviorOnError.LOG.name(), BehaviorOnError.IGNORE.name(),
                                BehaviorOnError.FAIL.name().toLowerCase(), BehaviorOnError.LOG.name().toLowerCase(),
                                BehaviorOnError.IGNORE.name().toLowerCase()),
                        Importance.LOW,
                        KUSTO_BEHAVIOR_ON_ERROR_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.LONG,
                        KUSTO_BEHAVIOR_ON_ERROR_DISPLAY)
                .define(
                        KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF,
                        Type.LIST,
                        "",
                        Importance.LOW,
                        KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_DLQ_BOOTSTRAP_SERVERS_DISPLAY)
                .define(
                        KUSTO_DLQ_TOPIC_NAME_CONF,
                        Type.STRING,
                        "",
                        Importance.LOW,
                        KUSTO_DLQ_TOPIC_NAME_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_DLQ_TOPIC_NAME_DISPLAY)
                .define(
                        KUSTO_SINK_MAX_RETRY_TIME_MS_CONF,
                        Type.LONG,
                        TimeUnit.SECONDS.toMillis(300),
                        Importance.LOW,
                        KUSTO_SINK_MAX_RETRY_TIME_MS_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_MAX_RETRY_TIME_MS_DISPLAY)
                .define(
                        KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF,
                        Type.LONG,
                        TimeUnit.SECONDS.toMillis(10),
                        ConfigDef.Range.atLeast(1),
                        Importance.LOW,
                        KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DISPLAY);
    }

    private static void defineWriteConfigs(ConfigDef result) {
        final String writeGroupName = "Writes";
        int writeGroupOrder = 0;

        result
                .define(
                        KUSTO_TABLES_MAPPING_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        KUSTO_TABLES_MAPPING_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_TABLES_MAPPING_DISPLAY)
                .define(
                        KUSTO_SINK_TEMP_DIR_CONF,
                        Type.STRING,
                        System.getProperty("java.io.tmpdir"),
                        Importance.LOW,
                        KUSTO_SINK_TEMP_DIR_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_TEMP_DIR_DISPLAY)
                .define(
                        KUSTO_SINK_FLUSH_SIZE_BYTES_CONF,
                        Type.LONG,
                        FileUtils.ONE_MB,
                        ConfigDef.Range.atLeast(100),
                        Importance.MEDIUM,
                        KUSTO_SINK_FLUSH_SIZE_BYTES_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_FLUSH_SIZE_BYTES_DISPLAY)
                .define(
                        KUSTO_SINK_FLUSH_INTERVAL_MS_CONF,
                        Type.LONG,
                        TimeUnit.SECONDS.toMillis(30),
                        ConfigDef.Range.atLeast(100),
                        Importance.HIGH,
                        KUSTO_SINK_FLUSH_INTERVAL_MS_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY);
    }

    private static void defineConnectionConfigs(ConfigDef result) {
        final String connectionGroupName = "Connection";
        int connectionGroupOrder = 0;

        result
                .define(
                        KUSTO_INGEST_URL_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        KUSTO_INGEST_URL_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_INGEST_URL_DISPLAY)
                .define(
                        KUSTO_ENGINE_URL_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.LOW,
                        KUSTO_ENGINE_URL_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_ENGINE_URL_DISPLAY)
                .define(
                        KUSTO_AUTH_APPKEY_CONF,
                        Type.PASSWORD,
                        null,
                        Importance.HIGH,
                        KUSTO_AUTH_APPKEY_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_APPKEY_DISPLAY)
                .define(
                        KUSTO_AUTH_APPID_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        KUSTO_AUTH_APPID_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_APPID_DISPLAY)
                .define(
                        KUSTO_AUTH_AUTHORITY_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        KUSTO_AUTH_AUTHORITY_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_AUTHORITY_DISPLAY)
                .define(
                        KUSTO_SINK_ENABLE_TABLE_VALIDATION,
                        Type.BOOLEAN,
                        Boolean.TRUE,
                        Importance.LOW,
                        KUSTO_SINK_ENABLE_TABLE_VALIDATION_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.SHORT,
                        KUSTO_SINK_ENABLE_TABLE_VALIDATION_DISPLAY)
                .define(
                        KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_CONF,
                        Type.BOOLEAN,
                        Boolean.FALSE,
                        Importance.HIGH,
                        KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_DISPLAY);
    }

    public static void main(String[] args) {
        System.out.println(getConfig().toEnrichedRst());
    }

    public String getKustoIngestUrl() {
        return this.getString(KUSTO_INGEST_URL_CONF);
    }

    public String getKustoEngineUrl() {
        return this.getString(KUSTO_ENGINE_URL_CONF);
    }

    public String getAuthAppId() {
        return this.getString(KUSTO_AUTH_APPID_CONF);
    }

    public String getAuthAppKey() {
        return this.getPassword(KUSTO_AUTH_APPKEY_CONF).value();
    }

    public String getAuthAuthority() {
        return this.getString(KUSTO_AUTH_AUTHORITY_CONF);
    }

    public boolean getManagedIdentityEnabled() {
        return this.getBoolean(KUSTO_AUTH_MANAGED_IDENTITY_ENABLED_CONF);
    }

    public String getTopicToTableMapping() {
        return getString(KUSTO_TABLES_MAPPING_CONF);
    }

    public String getTempDirPath() {
        return getString(KUSTO_SINK_TEMP_DIR_CONF);
    }

    public long getFlushSizeBytes() {
        return getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF);
    }

    public long getFlushInterval() {
        return getLong(KUSTO_SINK_FLUSH_INTERVAL_MS_CONF);
    }

    public BehaviorOnError getBehaviorOnError() {
        return BehaviorOnError.valueOf(
                getString(KUSTO_BEHAVIOR_ON_ERROR_CONF).toUpperCase());
    }

    public boolean isDlqEnabled() {
        if (!getDlqBootstrapServers().isEmpty() && !Strings.isNullOrEmpty(getDlqTopicName())) {
            return true;
        } else if (getDlqBootstrapServers().isEmpty() && Strings.isNullOrEmpty(getDlqTopicName())) {
            return false;
        } else {
            throw new ConfigException("To enable Miscellaneous Dead-Letter Queue configuration please configure both " +
                    "`misc.deadletterqueue.bootstrap.servers` and `misc.deadletterqueue.topic.name` configurations ");
        }
    }

    public List<String> getDlqBootstrapServers() {
        return this.getList(KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF);
    }

    public String getDlqTopicName() {
        return getString(KUSTO_DLQ_TOPIC_NAME_CONF);
    }

    public Properties getDlqProps() {
        Map<String, Object> dlqconfigs = originalsWithPrefix(DLQ_PROPS_PREFIX);
        Properties props = new Properties();
        props.putAll(dlqconfigs);
        props.put("bootstrap.servers", getDlqBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    public long getMaxRetryAttempts() {
        return this.getLong(KUSTO_SINK_MAX_RETRY_TIME_MS_CONF)
                / this.getLong(KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF);
    }

    public long getRetryBackOffTimeMs() {
        return this.getLong(KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF);
    }

    public boolean getEnableTableValidation() {
        return this.getBoolean(KUSTO_SINK_ENABLE_TABLE_VALIDATION);
    }

    enum BehaviorOnError {
        FAIL, LOG, IGNORE;

        /**
         * Gets names of available behavior on error mode.
         *
         * @return array of available behavior on error mode names
         */
        public static String[] getNames() {
            return Arrays
                    .stream(BehaviorOnError.class.getEnumConstants())
                    .map(Enum::name)
                    .toArray(String[]::new);
        }
    }
}
