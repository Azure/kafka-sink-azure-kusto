package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class KustoSinkConfig extends AbstractConfig {



  enum ErrorTolerance {
      ALL, NONE;
    
    /**
     * Gets names of available enum.
     * @return array of available enum
     */
    public static String[] getNames() {
      return Arrays
              .stream(ErrorTolerance.class.getEnumConstants())
              .map(Enum::name)
              .toArray(String[]::new);
    }
  }
  
    // TODO: this might need to be per kusto cluster...
    static final String KUSTO_URL_CONF = "kusto.url";
    private static final String KUSTO_URL_DOC = "Kusto cluster.";
    private static final String KUSTO_URL_DISPLAY = "Kusto URL";

    static final String KUSTO_INGESTION_URL_CONF = "kusto.ingest.url";
    private static final String KUSTO_INGESTION_URL_DOC = "Kusto cluster url for ingestion.";
    private static final String KUSTO_INGESTION_URL_DISPLAY = "Kusto Ingestion URL";
    
    static final String KUSTO_AUTH_USERNAME_CONF = "kusto.auth.username";
    private static final String KUSTO_AUTH_USERNAME_DOC = "Kusto username for authentication, also configure kusto.auth.password.";
    private static final String KUSTO_AUTH_USERNAME_DISPLAY = "Kusto Auth Username";
    
    static final String KUSTO_AUTH_PASSWORD_CONF = "kusto.auth.password";
    private static final String KUSTO_AUTH_PASSWORD_DOC = "Kusto password for the configured username.";
    private static final String KUSTO_AUTH_PASSWORD_DISPLAY = "Kusto Auth Password";
    
    static final String KUSTO_AUTH_APPID_CONF = "kusto.auth.appid";
    private static final String KUSTO_AUTH_APPID_DOC = "Configure this to perform authentication using Kusto AppID, "
        + "also configure kusto.auth.appkey and kusto.auth.authority.";
    private static final String KUSTO_AUTH_APPID_DISPLAY = "Kusto Auth AppID";
    
    static final String KUSTO_AUTH_APPKEY_CONF = "kusto.auth.appkey";
    private static final String KUSTO_AUTH_APPKEY_DOC = "Kusto AppKey for authentication.";
    private static final String KUSTO_AUTH_APPKEY_DISPLAY = "Kusto Auth AppKey";
    
    static final String KUSTO_AUTH_AUTHORITY_CONF = "kusto.auth.authority";
    private static final String KUSTO_AUTH_AUTHORITY_DOC = "Kusto authority for authentication.";
    private static final String KUSTO_AUTH_AUTHORITY_DISPLAY = "Kusto Auth Authority";
    
    static final String KUSTO_TABLES_MAPPING_CONF = "kusto.tables.topics.mapping";
    private static final String KUSTO_TABLES_MAPPING_DOC = "Kusto target tables mapping (per topic mapping, "
        + "'topic1:table1;topic2:table2;').";
    private static final String KUSTO_TABLES_MAPPING_DISPLAY = "Kusto Table Topics Mapping";
    
    static final String KUSTO_SINK_TEMP_DIR_CONF = "tempdir.path";
    private static final String KUSTO_SINK_TEMP_DIR_DOC = "Temp dir that will be used by kusto sink to buffer records. "
        + "defaults to system temp dir.";
    private static final String KUSTO_SINK_TEMP_DIR_DISPLAY = "Temporary Directory";
    
    static final String KUSTO_SINK_FLUSH_SIZE_BYTES_CONF = "flush.size.bytes";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DOC = "Kusto sink max buffer size (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DISPLAY = "Maximum Flush Size";
    
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS_CONF = "flush.interval.ms";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DOC = "Kusto sink max staleness in milliseconds (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY = "Maximum Flush Interval";
    
    static final String KUSTO_SINK_ERROR_TOLERANCE_CONF = "error.tolerance";
    private static final String KUSTO_SINK_ERROR_TOLERANCE_DOC = "Error tolerance setting. "
        + "Must be configured to one of the following:\n"
        + "``NONE``\n"
        + "The Connector throws ConnectException and stops processing records "
        + "when an error occurs while processing or ingesting records into KustoDB.\n"
        + "``ALL``\n"
        + "Continues to process next subsequent records "
        + "when error occurs while processing or ingesting records into KustoDB.\n";
    private static final String KUSTO_SINK_ERROR_TOLERANCE_DISPLAY = "Error Tolerance";
    
    static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF = "dlq.bootstrap.servers";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC = "Configure this to Kafka broker's address(es) "
        + "to which the Connector should write failed records to.";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DISPLAY = "Dead-Letter Queue Bootstrap Servers";
    
    static final String KUSTO_DLQ_TOPIC_NAME_CONF = "dlq.topic.name";
    private static final String KUSTO_DLQ_TOPIC_NAME_DOC = "Set this to Kafka topic's name "
        + "to which the failed records are to be sinked. "
        + "By default, the Connector will write failed records to {$origin-topic}-failed. "
        + "The Connector will create the topic if not already present with replication factor as 1. "
        + "To configure this to a desirable value, create topic from CLI prior to running the Connector.";
    private static final String KUSTO_DLQ_TOPIC_NAME_DISPLAY = "Dead-Letter Queue Topic Name";
    
    static final String KUSTO_SINK_MAX_RETRY_TIME_MS_CONF = "max.retry.time.ms";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DOC = "Maximum time upto which the Connector "
        + "should retry writing records to KustoDB in case of failures.";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DISPLAY = "Maximum Retry Time";
    
    static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF = "retry.backoff.time.ms";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC = "BackOff time between retry attempts "
        + "the Connector makes to ingest records into KustoDB.";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DISPLAY = "Retry BackOff Time";

    private static final String KUSTO_SINK_AUTO_TABLE_CREATE = "kusto.sink.auto_table_create";
    
    
    public KustoSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public KustoSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
      
      final String connectionGroupName = "Connection";
      final String writeGroupName = "Writes";
      final String errorAndRetriesGroupName = "Error Handling and Retries";
      
      int connectionGroupOrder = 0;
      int writeGroupOrder = 0;
      int errorAndRetriesGroupOrder = 0;
      
      //TODO: Add display name, validators, recommenders to configs.
        return new ConfigDef()
            .define(
                KUSTO_URL_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                KUSTO_URL_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_URL_DISPLAY)
            .define(
                    KUSTO_INGESTION_URL_CONF,
                    Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    KUSTO_INGESTION_URL_DOC,
                    connectionGroupName,
                    connectionGroupOrder++,
                    Width.MEDIUM,
                    KUSTO_INGESTION_URL_DISPLAY)
            .define(
                KUSTO_AUTH_USERNAME_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                KUSTO_AUTH_USERNAME_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_USERNAME_DISPLAY)
            .define(
                KUSTO_AUTH_PASSWORD_CONF,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                KUSTO_AUTH_PASSWORD_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_PASSWORD_DISPLAY)
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
                Type.PASSWORD,
                null,
                Importance.HIGH,
                KUSTO_AUTH_APPID_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_APPID_DISPLAY)
            .define(
                KUSTO_AUTH_AUTHORITY_CONF,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                KUSTO_AUTH_AUTHORITY_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_AUTHORITY_DISPLAY)
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
                System.getProperty("java.io.tempdir"),
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
                TimeUnit.SECONDS.toMillis(300),
                ConfigDef.Range.atLeast(100),
                Importance.HIGH,
                KUSTO_SINK_FLUSH_INTERVAL_MS_DOC,
                writeGroupName,
                writeGroupOrder++,
                Width.MEDIUM,
                KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY)
            .define(
                KUSTO_SINK_ERROR_TOLERANCE_CONF,
                Type.STRING,
                ErrorTolerance.NONE.name(),
                ConfigDef.ValidString.in("ALL", "NONE"),
                Importance.LOW,
                KUSTO_SINK_ERROR_TOLERANCE_DOC,
                errorAndRetriesGroupName,
                errorAndRetriesGroupOrder++,
                Width.LONG,
                KUSTO_SINK_ERROR_TOLERANCE_DISPLAY)
            .define(
                KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF,
                Type.STRING,
                null,
                Importance.LOW,
                KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC,
                errorAndRetriesGroupName,
                errorAndRetriesGroupOrder++,
                Width.MEDIUM,
                KUSTO_DLQ_BOOTSTRAP_SERVERS_DISPLAY)
            .define(
                KUSTO_DLQ_TOPIC_NAME_CONF,
                Type.STRING,
                null,
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
                Importance.LOW,
                KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC,
                errorAndRetriesGroupName,
                errorAndRetriesGroupOrder++,
                Width.MEDIUM,
                KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DISPLAY);    
        }


    public String getKustoUrl() {
        return this.getString(KUSTO_URL_CONF);
    }

    public String getKustoIngestUrl(){
        return this.getString(KUSTO_INGESTION_URL_CONF);
    }

    public String getAuthUsername() {
        return this.getString(KUSTO_AUTH_USERNAME_CONF);
    }

    public String getAuthPassword() {
        return this.getString(KUSTO_AUTH_PASSWORD_CONF);
    }
  
    public String getKustoAuthAppid() {
        return this.getString(KUSTO_AUTH_APPID_CONF);
    }
  
    public String getAuthAppkey() {
        return this.getString(KUSTO_AUTH_APPKEY_CONF);
    }
  
    public String getAuthAuthority() {
        return this.getString(KUSTO_AUTH_AUTHORITY_CONF);
    }
  
    public String getTopicToTableMapping() {
        return this.getString(KUSTO_TABLES_MAPPING_CONF);
    }

    public String getTempDirPath() {
        return this.getString(KUSTO_SINK_TEMP_DIR_CONF);
    }

    public long getFlushSizeBytes() {
        return this.getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF);
    }

    public long getFlushInterval() {
        return this.getLong(KUSTO_SINK_FLUSH_INTERVAL_MS_CONF);
    }

    public String getErrorTolerance() {
        return this.getString(KUSTO_SINK_TEMP_DIR_CONF);
    }
    
    public String getDlqBootstrapServers() {
        return this.getString(KUSTO_SINK_TEMP_DIR_CONF);
    }
    
    public String getDlqTopicName() {
        return this.getString(KUSTO_SINK_TEMP_DIR_CONF);
    }
    
    public long getMaxRetryTime() {
        return this.getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF);
    }
    
    public long getBackOffTime() {
        return this.getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF);
    }
    
    public static void main(String[] args) {
      System.out.println(getConfig().toEnrichedRst());
    }

    public boolean getKustoAutoTableCreate() {
        return this.getBoolean(KUSTO_SINK_AUTO_TABLE_CREATE);
    }
}

