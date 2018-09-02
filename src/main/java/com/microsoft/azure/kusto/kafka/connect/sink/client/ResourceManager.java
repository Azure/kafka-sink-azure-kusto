package com.microsoft.azure.kusto.kafka.connect.sink.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ResourceManager {

    // Constants:
    private static final String SECURED_READY_FOR_AGGREGATION_QUEUE = "SecuredReadyForAggregationQueue";
    private static final String FAILED_INGESTIONS_QUEUE = "FailedIngestionsQueue";
    private static final String SUCCESSFUL_INGESTIONS_QUEUE = "SuccessfulIngestionsQueue";
    private static final String TEMP_STORAGE = "TempStorage";
    private static final String INGESTIONS_STATUS_TABLE = "IngestionsStatusTable";

    // Ingestion Resources value lists:
    private ArrayList<String> aggregationQueueList = new ArrayList<>();
    private ArrayList<String> failedIngestionsQueueList = new ArrayList<>();
    private ArrayList<String> successfulIngestionsQueueList = new ArrayList<>();
    private ArrayList<String> tempStorageList = new ArrayList<>();
    private ArrayList<String> ingestionsStatusTableList = new ArrayList<>();

    //Identity Token
    private String m_identityToken;

    // Round-rubin indexes:
    private int aggregationQueueIdx = 0;
    private int tempStorageIdx = 0;

    private KustoClient m_kustoClient;
    private final long refreshIngestionResourcesPeriod = 1000 * 60 * 60 * 1; // 1 hour
    private Timer timer = new Timer(true);
    private final Logger log = LoggerFactory.getLogger(KustoIngestClient.class);

    public ResourceManager(KustoClient kustoClient) {
        m_kustoClient = kustoClient;

        TimerTask refreshIngestionResourceValuesTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshIngestionResources();
                } catch (Exception e) {
                    log.error(String.format("Error in refreshIngestionResources: %s.", e.getMessage()), e);
                }
            }
        };

        TimerTask refreshIngestionAuthTokenTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshIngestionAuthToken();
                } catch (Exception e) {
                    log.error(String.format("Error in refreshIngestionAuthToken: %s.", e.getMessage()), e);
                }
            }
        };

        timer.schedule(refreshIngestionAuthTokenTask, 0, refreshIngestionResourcesPeriod);
        timer.schedule(refreshIngestionResourceValuesTask, 0, refreshIngestionResourcesPeriod);

    }

    public void clean() {
        aggregationQueueList = new ArrayList<>();
        failedIngestionsQueueList = new ArrayList<>();
        successfulIngestionsQueueList = new ArrayList<>();
        tempStorageList = new ArrayList<>();
        ingestionsStatusTableList = new ArrayList<>();
    }

    public String getKustoIdentityToken() throws Exception {
        if (m_identityToken == null) {
            refreshIngestionAuthToken();
            if (m_identityToken == null) {
                throw new Exception("Unable to get Identity token");
            }
        }
        return m_identityToken;
    }

    public String getStorageUri() throws Exception {
        int arrSize = tempStorageList.size();
        if (arrSize == 0) {
            refreshIngestionResources();
            arrSize = tempStorageList.size();
            if (arrSize == 0) {
                throw new Exception("Unable to get temp storages list");
            }
        }

        // Round-rubin over the values of tempStorageList:
        tempStorageIdx = (tempStorageIdx + 1) % arrSize;
        return tempStorageList.get(tempStorageIdx);
    }

    public String getAggregationQueue() throws Exception {
        int arrSize = aggregationQueueList.size();
        if (arrSize == 0) {
            refreshIngestionResources();
            arrSize = aggregationQueueList.size();
            if (arrSize == 0) {
                throw new Exception("Unable to get aggregation queues list");
            }
        }

        // Round-rubin over the values of aggregationQueueList:
        aggregationQueueIdx = (aggregationQueueIdx + 1) % arrSize;
        return aggregationQueueList.get(aggregationQueueIdx);
    }

    private void addValue(String key, String value) {
        switch (key) {
            case SECURED_READY_FOR_AGGREGATION_QUEUE:
                aggregationQueueList.add(value);
                break;
            case FAILED_INGESTIONS_QUEUE:
                failedIngestionsQueueList.add(value);
                break;
            case SUCCESSFUL_INGESTIONS_QUEUE:
                successfulIngestionsQueueList.add(value);
                break;
            case TEMP_STORAGE:
                tempStorageList.add(value);
                break;
            case INGESTIONS_STATUS_TABLE:
                ingestionsStatusTableList.add(value);
                break;
            default:
                log.warn("Unrecognized key: %s", key);
                break;
        }
    }


    private void refreshIngestionResources() throws Exception {
        log.info("Refreshing Ingestion Resources");
        KustoResults ingestionResourcesResults = m_kustoClient.execute(CslCommandGenerator.generateIngestionResourcesShowCommand());
        ArrayList<ArrayList<String>> values = ingestionResourcesResults.getValues();

        clean();

        values.forEach(pairValues -> {
            String key = pairValues.get(0);
            String value = pairValues.get(1);
            addValue(key, value);
        });
    }

    private void refreshIngestionAuthToken() throws Exception {
        log.info("Refreshing Ingestion Auth Token");
        KustoResults authToken = m_kustoClient.execute(CslCommandGenerator.generateKustoIdentityGetCommand());
        m_identityToken = authToken.getValues().get(0).get(authToken.getIndexByColumnName("AuthorizationContext"));
    }


    public ArrayList<String> getAggregationQueueList() {
        return aggregationQueueList;
    }

    public ArrayList<String> getFailedIngestionsQueueList() {
        return failedIngestionsQueueList;
    }

    public ArrayList<String> getSuccessfulIngestionsQueueList() {
        return successfulIngestionsQueueList;
    }

    public ArrayList<String> getTempStorageList() {
        return tempStorageList;
    }

    public ArrayList<String> getIngestionsStatusTableList() {
        return ingestionsStatusTableList;
    }
}