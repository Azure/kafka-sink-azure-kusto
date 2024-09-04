package com.microsoft.azure.kusto.kafka.connect.sink.it;

import org.apache.commons.lang3.StringUtils;

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
        this.authority = StringUtils.defaultIfBlank(authority, "microsoft.com");
        this.ingestCluster = ingestCluster;
        this.cluster = cluster;
        this.database = database;
        this.table = table;
    }

    boolean isValidConfig() {
        return StringUtils.isNotEmpty(authority) && StringUtils.isNotEmpty(cluster)
                && StringUtils.isNotEmpty(ingestCluster);
    }
}
