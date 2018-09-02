package com.microsoft.azure.kusto.kafka.connect.sink.client;

import org.json.JSONObject;

public class KustoClient {

    private final String c_adminCommandsPrefix = ".";
    private final String c_apiVersion = "v1";
    private final String c_defaultDatabaseName = "NetDefaultDb";

    private AadAuthenticationHelper m_aadAuthenticationHelper;
    private String m_clusterUrl;

    public KustoClient(KustoConnectionStringBuilder kcsb) throws Exception {
        m_clusterUrl = kcsb.getClusterUrl();
        m_aadAuthenticationHelper = new AadAuthenticationHelper(kcsb);
    }

    public KustoResults execute(String command) throws Exception {
        return execute(c_defaultDatabaseName, command);
    }

    public KustoResults execute(String database, String command) throws Exception {
        String clusterEndpoint;
        if (command.startsWith(c_adminCommandsPrefix)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", m_clusterUrl, c_apiVersion);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", m_clusterUrl, c_apiVersion);
        }
        return execute(database, command, clusterEndpoint);
    }

    private KustoResults execute(String database, String command, String clusterEndpoint) throws Exception {
        String aadAccessToken = m_aadAuthenticationHelper.acquireAccessToken();

        String jsonString = new JSONObject()
                .put("db", database)
                .put("csl", command).toString();

        return Utils.post(clusterEndpoint, aadAccessToken, jsonString);
    }
}