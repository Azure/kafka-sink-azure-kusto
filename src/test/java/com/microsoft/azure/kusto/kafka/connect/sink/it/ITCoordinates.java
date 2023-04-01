package com.microsoft.azure.kusto.kafka.connect.sink.it;

import org.apache.commons.lang3.StringUtils;

class ITCoordinates {

    String appId;
    String appKey;
    String authority;
    String cluster;
    String database;

    String table;

    ITCoordinates(String appId, String appKey, String authority, String cluster, String database, String table) {
        this.appId = appId;
        this.appKey = appKey;
        this.authority = authority;
        this.cluster = cluster;
        this.database = database;
        this.table = table;
    }

    boolean isValidConfig() {
        return StringUtils.isNotEmpty(appId) && StringUtils.isNotEmpty(appKey) && StringUtils.isNotEmpty(authority) && StringUtils.isNotEmpty(cluster);
    }
}
