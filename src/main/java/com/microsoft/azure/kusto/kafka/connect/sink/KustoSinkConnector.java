package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KustoSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkConnector.class);
    private Map<String, String> configProps;

    public KustoSinkConnector() {
        // No-arg constructor. It is instantiated by Connect framework.
        log.info("KustoSinkConnector initialized");
    }

    KustoSinkConnector(KustoSinkConfig config) {
        // No-arg constructor. It is instantiated by Connect framework.
        log.info("KustoSinkConnector initialized with config");
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KustoSinkTask.class;
    }


    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            configProps = new HashMap<>(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start KustoSinkConnector due to configuration error", e);
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(configProps);
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Shutting down KustoSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return KustoSinkConfig.getConfig();
    }
}