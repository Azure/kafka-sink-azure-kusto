package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KustoSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkConnector.class);
    
    private KustoSinkConfig config;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting KustoSinkConnector.");
        config = new KustoSinkConfig(props);
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
      
        if (maxTasks == 0) {
            log.warn("No Connector tasks have been configured.");
        }
        List<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(config.originalsStrings());
        for (int i = 0; i < maxTasks; i++) {
          configs.add(taskProps);
        }
        return configs;
    }
    
    @Override
    public void stop() {
        log.info("Shutting down KustoSinkConnector");
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