package com.microsoft.azure.kusto.kafka.connect.sink.it;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredentialBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.StringUtils;
import java.util.Collections;
import java.util.UUID;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;

public class ITSetup {

    public static final String LISTENER_ADDRESS = "kafka:19092";
    public static final String BOOTSTRAP_ADDRESS = "PLAINTEXT://%s".formatted(LISTENER_ADDRESS);
    public static final String CONFLUENT_VERSION = "8.0.0";
    public static final int KAFKA_CONNECT_PORT = 8083;
    public static final int SR_PORT = 8081;
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);


    static @NotNull ITCoordinates getConnectorProperties() {
        String testPrefix = "tmpKafkaSinkIT_";
        String appId = getProperty("appId", "", false);
        String appKey = getProperty("appKey", "", false);
        String authority = getProperty("authority", "", false);
        String cluster = getProperty("cluster", "", false);
        String ingestCluster = getProperty("ingest", "", false);
        String database = getProperty("database", "e2e", true);
        String defaultTable = testPrefix + UUID.randomUUID().toString().replace('-', '_');
        String table = getProperty("table", defaultTable, true);
        return new ITCoordinates(appId, appKey, authority, getAccessToken(cluster), cluster, ingestCluster, database, table);
    }

    private static String getAccessToken(String cluster) {
        String clusterScope = "%s/.default".formatted(cluster);
        TokenRequestContext tokenRequestContext = new TokenRequestContext()
                .setScopes(Collections.singletonList(clusterScope));
        AccessToken accessTokenObj = new AzureCliCredentialBuilder().build().getTokenSync(tokenRequestContext);
        return accessTokenObj.getToken();
    }

    private static String getProperty(String attribute, String defaultValue, boolean sanitize) {
        String value = System.getProperty(attribute);
        if (value == null) {
            value = System.getenv(attribute);
        }
        // In some cases we want a default value (for example DB name). The mandatory ones are checked in the IT before set-up
        value = StringUtils.isEmpty(value) ? defaultValue : value;
        return sanitize ? FilenameUtils.normalizeNoEndSeparator(value) : value;
    }

}
