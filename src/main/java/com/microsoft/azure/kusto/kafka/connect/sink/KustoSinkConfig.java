package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class KustoSinkConfig extends AbstractConfig {
    // TODO: this might need to be per kusto cluster...
    static final String KUSTO_URL = "kusto.url";
    static final String KUSTO_TABLES_MAPPING = "kusto.tables.topics_mapping";

    static final String KUSTO_AUTH_USERNAME = "kusto.auth.username";
    static final String KUSTO_AUTH_PASSWORD = "kusto.auth.password";

    static final String KUSTO_AUTH_APPID = "kusto.auth.appid";
    static final String KUSTO_AUTH_APPKEY = "kusto.auth.appkey";
    static final String KUSTO_AUTH_AUTHORITY = "kusto.auth.authority";

    static final String KUSTO_SINK_TEMPDIR = "kusto.sink.tempdir";
    static final String KUSTO_SINK_FLUSH_SIZE = "kusto.sink.flush_size";
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS = "kusto.sink.flush_interval_ms";
    static final String KUSTO_SINK_WRITE_TO_FILES = "kusto.sink.write_to_files";
    static final String KUSTO_SINK_AUTO_TABLE_CREATE = "kusto.sink.auto_table_create";

    public KustoSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public KustoSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
        return new ConfigDef()
                .define(KUSTO_URL, Type.STRING, Importance.HIGH, "Kusto cluster url")
                .define(KUSTO_TABLES_MAPPING, Type.STRING, null, Importance.HIGH, "Kusto target tables mapping (per topic mapping, 'topic1:table1;topic2:table2;')")
                .define(KUSTO_AUTH_USERNAME, Type.PASSWORD, null, Importance.HIGH, "Kusto auth using username,password combo: username")
                .define(KUSTO_AUTH_PASSWORD, Type.PASSWORD, null, Importance.HIGH, "Kusto auth using username,password combo: password")
                .define(KUSTO_AUTH_APPID, Type.PASSWORD, null, Importance.HIGH, "Kusto auth using appid,appkey combo: app id")
                .define(KUSTO_AUTH_APPKEY, Type.PASSWORD, null, Importance.HIGH, "Kusto auth using appid,appkey combo:  app key")
                .define(KUSTO_AUTH_AUTHORITY, Type.PASSWORD, null, Importance.HIGH, "Kusto auth using appid,appkey combo: authority")
                .define(KUSTO_SINK_TEMPDIR, Type.STRING, System.getProperty("java.io.tempdir"), Importance.LOW, "Temp dir that will be used by kusto sink to buffer records. defaults to system temp dir")
                .define(KUSTO_SINK_FLUSH_SIZE, Type.LONG, FileUtils.ONE_MB, Importance.HIGH, "Kusto sink max buffer size (per topic+partition combo)")
                .define(KUSTO_SINK_FLUSH_INTERVAL_MS, Type.LONG, TimeUnit.MINUTES.toMillis(5), Importance.HIGH, "Kusto sink max staleness in milliseconds (per topic+partition combo)")
                .define(KUSTO_SINK_AUTO_TABLE_CREATE, Type.BOOLEAN, false, Importance.LOW,"If true, allows the connector to create the table in Kusto if it not already exists.");
    }

    public String getKustoUrl() {
        return this.getString(KUSTO_URL);
    }

    public String getKustoTopicToTableMapping() {
        return this.getString(KUSTO_TABLES_MAPPING);
    }

    public String getKustoAuthUsername() {
        return this.getString(KUSTO_AUTH_USERNAME);
    }

    public String getKustoAuthPassword() {
        return this.getString(KUSTO_AUTH_PASSWORD);
    }

    public String getKustoAuthAppid() {
        return this.getString(KUSTO_AUTH_APPID);
    }

    public String getKustoAuthAppkey() {
        return this.getString(KUSTO_AUTH_APPKEY);
    }

    public String getKustoAuthAuthority() {
        return this.getString(KUSTO_AUTH_AUTHORITY);
    }

    public long getKustoFlushSize() {
        return this.getLong(KUSTO_SINK_FLUSH_SIZE);
    }

    public String getKustoSinkTempDir() {
        return this.getString(KUSTO_SINK_TEMPDIR);
    }

    public long getKustoFlushIntervalMS() {
        return this.getLong(KUSTO_SINK_FLUSH_INTERVAL_MS);
    }

    public boolean getKustoAutoTableCreate() {
        return this.getBoolean(KUSTO_SINK_AUTO_TABLE_CREATE);
    }
}

