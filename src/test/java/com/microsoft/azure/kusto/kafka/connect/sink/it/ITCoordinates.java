package com.microsoft.azure.kusto.kafka.connect.sink.it;

import com.microsoft.azure.kusto.data.StringUtils;

class ITCoordinates {

    final String appId;
    final String appKey;
    final String authority;
    final String accessToken;
    final String cluster;
    final String ingestCluster;
    final String database;

    String table;

    ITCoordinates(String appId, String appKey, String authority, String accessToken, String cluster,
            String ingestCluster, String database, String table) {
        this.appId = appId;
        this.appKey = appKey;
        this.accessToken = accessToken;
        this.authority = StringUtils.isEmpty(authority)?"microsoft.com":authority;
        this.ingestCluster = ingestCluster;
        this.cluster = cluster;
        this.database = database;
        this.table = table;
    }

    boolean isValidConfig() {
        return StringUtils.isNotBlank(authority) && StringUtils.isNotBlank(cluster)
                && StringUtils.isNotBlank(ingestCluster);
    }
}
