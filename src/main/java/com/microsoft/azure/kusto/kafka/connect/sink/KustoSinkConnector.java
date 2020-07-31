package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KustoSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkConnector.class);
    
    private KustoSinkConfig config;
    
    private String tempDirPath;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting KustoSinkConnector.");
        config = new KustoSinkConfig(props);
        createTempDirectory(config.getTempDirPath());
    }
    
    /*
     * Common temporary directory for the Connector tasks 
     * to write all temp files for ingestion.
     */
    private void createTempDirectory(String tempDirPath) {
        String systemTempDirPath = tempDirPath;
        String tempDir = "kusto-sink-connector-" + UUID.randomUUID().toString();
        Path path = Paths.get(systemTempDirPath, tempDir);
    
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new ConfigException("Failed to create temp directory=" + tempDir, e);
        }
        this.tempDirPath = path.toString();
   }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
      
        if (maxTasks == 0) {
            log.warn("No Connector tasks have been configured.");
        }
        List<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(config.originalsStrings());
        taskProps.put(KustoSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, tempDirPath);
        for (int i = 0; i < maxTasks; i++) {
          configs.add(taskProps);
        }
        return configs;
    }
    
    @Override
    public void stop() {
        log.info("Shutting down KustoSinkConnector");
        try {
            FileUtils.deleteDirectory(new File(tempDirPath));
        } catch (IOException e) {
            log.error("Unable to delete temporary connector folder {}", config.getTempDirPath());
        }
    }
    
    @Override
    public ConfigDef config() {
        return KustoSinkConfig.getConfig();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KustoSinkTask.class;
    }
    
    @Override
    public String version() {
        return Version.getVersion();
    }
}