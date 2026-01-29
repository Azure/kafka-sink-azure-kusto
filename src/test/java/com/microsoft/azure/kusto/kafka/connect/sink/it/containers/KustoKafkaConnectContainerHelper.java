package com.microsoft.azure.kusto.kafka.connect.sink.it.containers;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.KAFKA_CONNECT_PORT;
import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

public class KustoKafkaConnectContainerHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KustoKafkaConnectContainerHelper.class);

    private static final Duration KAFKA_CONNECT_START_TIMEOUT = Duration.ofMinutes(1);

    private final GenericContainer<?> kafkaConnectContainer;

    public KustoKafkaConnectContainerHelper(GenericContainer<?> kafkaConnectContainer) {
        this.kafkaConnectContainer = kafkaConnectContainer;
    }

    private static void handleFailedResponse(@NotNull HttpResponse response) throws IOException {
        String responseBody = EntityUtils.toString(response.getEntity());
        LOGGER.error("Error registering connector with error {}", responseBody);
        throw new RuntimeException("Error registering connector with error " + responseBody);
    }

    public String getTarget() {
        return "http://" + kafkaConnectContainer.getHost() + ":" + kafkaConnectContainer.getMappedPort(KAFKA_CONNECT_PORT);
        // return "http://" + getContainerId().substring(0, 12) + ":" + getMappedPort(KAFKA_CONNECT_PORT);
    }

    public void registerConnector(String name, Map<String, Object> configuration) {
        try {
            Map<String, Object> connectorConfiguration = new HashMap<>();
            connectorConfiguration.put("name", name);
            connectorConfiguration.put("config", configuration);
            String postConfig = OBJECT_MAPPER.writeValueAsString(connectorConfiguration);
            LOGGER.trace("Registering connector {} with config {}", name, postConfig);
            executePOSTRequestSuccessfully(postConfig, "%s/connectors".formatted(getTarget()));
            Awaitility.await()
                    .atMost(KAFKA_CONNECT_START_TIMEOUT)
                    .until(() -> isConnectorConfigured(name));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isConnectorConfigured(String connectorName) {
        // HTTP get request to check if connector is configured
        URI connectorUri = URI.create("%s/connectors/%s/status".formatted(getTarget(), connectorName));
        HttpGet httpget = new HttpGet(connectorUri);
        try (CloseableHttpClient httpclient = HttpClients.createDefault();
                CloseableHttpResponse httpResponse = httpclient.execute(httpget)) {
            int responseCode = httpResponse.getStatusLine().getStatusCode();
            return 200 <= responseCode && responseCode <= 300;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void executePOSTRequestSuccessfully(final String payload, final String fullUrl) {
        final HttpPost httpPost = new HttpPost(URI.create(fullUrl));
        final StringEntity entity = new StringEntity(payload, StandardCharsets.UTF_8);
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        try (CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client
                        .execute(httpPost)) {
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 201) {
                handleFailedResponse(response);
            }
        } catch (IOException e) {
            LOGGER.error("Error registering connector, exception when invoking endpoint {}", fullUrl, e);
        }
    }

    public String getConnectorTaskState(String connectorName, int taskNumber) {
        // HTTP get request to check if connector is configured
        URI statusUri = URI.create("%s/connectors/%s/tasks/%d/status".formatted(getTarget(), connectorName, taskNumber));
        HttpGet httpget = new HttpGet(statusUri);
        try (CloseableHttpClient httpclient = HttpClients.createDefault();
                CloseableHttpResponse httpResponse = httpclient.execute(httpget)) {
            int responseCode = httpResponse.getStatusLine().getStatusCode();
            if (200 <= responseCode && responseCode <= 300) {
                try {
                    String responseBody = EntityUtils.toString(httpResponse.getEntity());
                    Map<?, ?> responseMap = OBJECT_MAPPER.readValue(responseBody, Map.class);
                    String connectorState = (String) responseMap.get("state");
                    LOGGER.info("Connector {} task {} state is {}", connectorName, taskNumber, connectorState);
                    return connectorState;
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitUntilConnectorTaskStateChanges(String connectorName, int taskNumber, String status) {
        Awaitility.await()
                .atMost(KAFKA_CONNECT_START_TIMEOUT)
                .until(() -> status.equalsIgnoreCase(getConnectorTaskState(connectorName, taskNumber)));
    }
}
