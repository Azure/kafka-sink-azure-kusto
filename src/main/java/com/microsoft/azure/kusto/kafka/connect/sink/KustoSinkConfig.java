package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.testng.util.Strings;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class KustoSinkConfig extends AbstractConfig {
  
    // TODO: this might need to be per kusto cluster...
    static final String KUSTO_URL_CONF = "kusto.url";
    private static final String KUSTO_URL_DOC = "Kusto ingestion service URI.";
    private static final String KUSTO_URL_DISPLAY = "Kusto cluster URI";

    static final String KUSTO_AUTH_USERNAME_CONF = "kusto.auth.username";
    private static final String KUSTO_AUTH_USERNAME_DOC = "Kusto username for authentication, also configure kusto.auth.password.";
    private static final String KUSTO_AUTH_USERNAME_DISPLAY = "Kusto Auth Username";
    
    static final String KUSTO_AUTH_PASSWORD_CONF = "kusto.auth.password";
    private static final String KUSTO_AUTH_PASSWORD_DOC = "Kusto password for the configured username.";
    private static final String KUSTO_AUTH_PASSWORD_DISPLAY = "Kusto Auth Password";
    
    static final String KUSTO_AUTH_APPID_CONF = "aad.auth.appid";
    private static final String KUSTO_AUTH_APPID_DOC = "Application Id for Azure Active Directory authentication.";
    private static final String KUSTO_AUTH_APPID_DISPLAY = "Kusto Auth AppID";
    
    static final String KUSTO_AUTH_APPKEY_CONF = "aad.auth.appkey";
    private static final String KUSTO_AUTH_APPKEY_DOC = "Application Key for Azure Active Directory authentication.";
    private static final String KUSTO_AUTH_APPKEY_DISPLAY = "Kusto Auth AppKey";
    
    static final String KUSTO_AUTH_AUTHORITY_CONF = "aad.auth.authority";
    private static final String KUSTO_AUTH_AUTHORITY_DOC = "Azure Active Directory tenant.";
    private static final String KUSTO_AUTH_AUTHORITY_DISPLAY = "Kusto Auth Authority";
    
    static final String KUSTO_TABLES_MAPPING_CONF = "kusto.tables.topics.mapping";
    private static final String KUSTO_TABLES_MAPPING_DOC = "Kusto target tables mapping (per topic mapping, "
        + "'topic1:table1;topic2:table2;').";
    private static final String KUSTO_TABLES_MAPPING_DISPLAY = "Kusto Table Topics Mapping";
    
    static final String KUSTO_SINK_TEMP_DIR_CONF = "kusto.sink.tempdir";
    private static final String KUSTO_SINK_TEMP_DIR_DOC = "Temp dir that will be used by kusto sink to buffer records. "
        + "defaults to system temp dir.";
    private static final String KUSTO_SINK_TEMP_DIR_DISPLAY = "Temporary Directory";
    
    static final String KUSTO_SINK_FLUSH_SIZE_BYTES_CONF = "flush.size.bytes";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DOC = "Kusto sink max buffer size (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DISPLAY = "Maximum Flush Size";
    
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS_CONF = "flush.interval.ms";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DOC = "Kusto sink max staleness in milliseconds (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY = "Maximum Flush Interval";
    
    // Deprecated configs
    static final String KUSTO_TABLES_MAPPING_CONF_DEPRECATED = "kusto.tables.topics_mapping";
    static final String KUSTO_SINK_FLUSH_SIZE_BYTES_CONF_DEPRECATED = "kusto.sink.flush_size";
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS_CONF_DEPRECATED = "kusto.sink.flush_interval_ms";
    
    private static final String DEPRECATED_CONFIG_DOC = "This configuration has been deprecated.";

    
    public KustoSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public KustoSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
      
      final String connectionGroupName = "Connection";
      final String writeGroupName = "Writes";
      
      int connectionGroupOrder = 0;
      int writeGroupOrder = 0;
      
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
                "",
                Importance.HIGH,
                KUSTO_AUTH_PASSWORD_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_PASSWORD_DISPLAY)
            .define(
                KUSTO_AUTH_APPKEY_CONF,
                Type.PASSWORD,
                "",
                Importance.HIGH,
                KUSTO_AUTH_APPKEY_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_APPKEY_DISPLAY)
            .define(
                KUSTO_AUTH_APPID_CONF,
                Type.PASSWORD,
                "",
                Importance.HIGH,
                KUSTO_AUTH_APPID_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_APPID_DISPLAY)
            .define(
                KUSTO_AUTH_AUTHORITY_CONF,
                Type.PASSWORD,
                "",
                Importance.HIGH,
                KUSTO_AUTH_AUTHORITY_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.MEDIUM,
                KUSTO_AUTH_AUTHORITY_DISPLAY)
            .define(
                KUSTO_TABLES_MAPPING_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                KUSTO_TABLES_MAPPING_DOC,
                writeGroupName,
                writeGroupOrder++,
                Width.MEDIUM,
                KUSTO_TABLES_MAPPING_DISPLAY)
            .define(
                KUSTO_TABLES_MAPPING_CONF_DEPRECATED,
                Type.STRING,
                null,
                Importance.HIGH,
                KUSTO_TABLES_MAPPING_DOC + DEPRECATED_CONFIG_DOC,
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
                KUSTO_SINK_FLUSH_SIZE_BYTES_CONF_DEPRECATED,
                Type.LONG,
                FileUtils.ONE_MB,
                ConfigDef.Range.atLeast(100),
                Importance.MEDIUM,
                KUSTO_SINK_FLUSH_SIZE_BYTES_DOC + DEPRECATED_CONFIG_DOC,
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
                KUSTO_SINK_FLUSH_INTERVAL_MS_CONF_DEPRECATED,
                Type.LONG,
                TimeUnit.SECONDS.toMillis(300),
                ConfigDef.Range.atLeast(100),
                Importance.HIGH,
                KUSTO_SINK_FLUSH_INTERVAL_MS_DOC + DEPRECATED_CONFIG_DOC,
                writeGroupName,
                writeGroupOrder++,
                Width.MEDIUM,
                KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY);
        }


    public String getKustoUrl() {
        return this.getString(KUSTO_URL_CONF);
    }

    public String getAuthUsername() {
        return this.getString(KUSTO_AUTH_USERNAME_CONF);
    }

    public String getAuthPassword() {
        return this.getPassword(KUSTO_AUTH_PASSWORD_CONF).value();
    }
  
    public String getAuthAppid() {
        return this.getPassword(KUSTO_AUTH_APPID_CONF).value();
    }
  
    public String getAuthAppkey() {
        return this.getPassword(KUSTO_AUTH_APPKEY_CONF).value();
    }
  
    public String getAuthAuthority() {
        return this.getPassword(KUSTO_AUTH_AUTHORITY_CONF).value();
    }
  
    public String getTopicToTableMapping() {
        // If the deprecated config is not set to default
        return (!Strings.isNullOrEmpty(getString(KUSTO_TABLES_MAPPING_CONF))) 
            ? getString(KUSTO_TABLES_MAPPING_CONF) 
            : getString(KUSTO_TABLES_MAPPING_CONF_DEPRECATED);
    }

    public String getTempDirPath() {
        return this.getString(KUSTO_SINK_TEMP_DIR_CONF);
    }

    public long getFlushSizeBytes() {
        // If the deprecated config is not set to default
        return (getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF_DEPRECATED) != FileUtils.ONE_MB)
            ? getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF_DEPRECATED)
            : getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF);
    }

    public long getFlushInterval() {
        // If the deprecated config is not set to default
        return (getLong(KUSTO_SINK_FLUSH_INTERVAL_MS_CONF_DEPRECATED) != TimeUnit.SECONDS.toMillis(300))
            ? getLong(KUSTO_SINK_FLUSH_INTERVAL_MS_CONF_DEPRECATED)
            : getLong(KUSTO_SINK_FLUSH_INTERVAL_MS_CONF);
    }

    public static void main(String[] args) {
        System.out.println(getConfig().toEnrichedRst());
    }
}

