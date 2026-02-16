package com.microsoft.azure.kusto.kafka.connect.sink.it;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.kafka.connect.sink.it.containers.KustoKafkaConnectContainerHelper;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.KAFKA_CONNECT_PORT;
import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.getConnectorProperties;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.fail;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

/**
 * Integration test using Docker Compose with Strimzi Kafka (KRaft mode).
 * Tests JSON format message handling with Strimzi Kafka distribution.
 */
@Testcontainers
class KustoSinkStrimziIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(KustoSinkStrimziIT.class);
    private static final String KEY_COLUMN = "id";
    private static final String TEST_TOPIC = "test.json.topic";
    private static final int KAFKA_PORT = 9092;
    private static final String KAFKA_SERVICE = "kafka";
    private static final String CONNECT_SERVICE = "kafka-connect";

    @Container
    private static final ComposeContainer COMPOSE_CONTAINER = new ComposeContainer(new File("src/test/resources/docker-compose.yml"))
            .withExposedService(KAFKA_SERVICE, KAFKA_PORT,
                    Wait.forLogMessage(".*Kafka Server started.*", 1)
                            .withStartupTimeout(Duration.ofSeconds(120)))
            .withExposedService(CONNECT_SERVICE, KAFKA_CONNECT_PORT,
                    Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1)
                            .withStartupTimeout(Duration.ofSeconds(180)))
            .withLocalCompose(true);

    private static KustoKafkaConnectContainerHelper kcHelper;
    // state variables for the test
    private static ITCoordinates coordinates;
    private static Client engineClient = null;
    private static Client dmClient = null;

    @BeforeAll
    public static void startContainers() throws Exception {
        coordinates = getConnectorProperties();
        if (coordinates.isValidConfig()) {
            ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
                    coordinates.cluster, coordinates.accessToken);
            ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
                    coordinates.ingestCluster, coordinates.accessToken);
            engineClient = ClientFactory.createClient(engineCsb);
            dmClient = ClientFactory.createClient(dmCsb);
            LOGGER.info("Creating tables in Kusto for Strimzi test");
            // Get Kafka Connect URL from docker-compose
            String serviceHost = COMPOSE_CONTAINER.getServiceHost(CONNECT_SERVICE, KAFKA_CONNECT_PORT);
            int serviceWithInstancePort = COMPOSE_CONTAINER.getServicePort(CONNECT_SERVICE, KAFKA_CONNECT_PORT);
            // Initialize the Kafka Connect helper with the compose container's kafka-connect service
            String connectUrl = "http://" + serviceHost + ":" + serviceWithInstancePort;
            kcHelper = new KustoKafkaConnectContainerHelper(connectUrl);
            createTables();
            refreshDm();
        } else {
            LOGGER.info("Skipping Strimzi test due to missing configuration");
        }
    }

    private static void createTables() {
        // Create a simple table for JSON data
        String tableName = coordinates.table + "_strimzi";
        String createTableCommand = String.format(".create table %s (id:long, name:string, value:double, timestamp:datetime)", tableName);
        String createMappingCommand = String.format(
                ".create table %s ingestion json mapping 'json_mapping' '[{\"column\":\"id\",\"path\":\"$.id\",\"datatype\":\"long\"}," +
                        "{\"column\":\"name\",\"path\":\"$.name\",\"datatype\":\"string\"}," +
                        "{\"column\":\"value\",\"path\":\"$.value\",\"datatype\":\"double\"}," +
                        "{\"column\":\"timestamp\",\"path\":\"$.timestamp\",\"datatype\":\"datetime\"}]'",
                tableName);
        try {
            engineClient.executeMgmt(coordinates.database, createTableCommand);
            LOGGER.info("Created table {}", tableName);
            engineClient.executeMgmt(coordinates.database, createMappingCommand);
            LOGGER.info("Created JSON mapping for table {}", tableName);
        } catch (Exception e) {
            LOGGER.error("Failed to create table or mapping", e);
            throw e;
        }
    }

    private static void refreshDm() throws Exception {
        URL kqlResource = KustoSinkStrimziIT.class.getClassLoader().getResource("dm-refresh-cache.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Path.of(kqlResource.toURI()))
                .stream()
                .map(kql -> kql.replace("TBL", coordinates.table + "_strimzi"))
                .map(kql -> kql.replace("DB", coordinates.database))
                .toList();
        kqlsToExecute.forEach(kql -> {
            try {
                dmClient.executeMgmt(kql);
            } catch (Exception e) {
                LOGGER.error("Failed to execute DM kql: {}", kql, e);
            }
        });
        LOGGER.info("Refreshed cache on DB {} for Strimzi test", coordinates.database);
    }

    @AfterAll
    public static void stopContainers() {
        if (coordinates != null && coordinates.isValidConfig()) {
            try {
                // DockerComposeContainer will automatically stop all services
                engineClient.executeMgmt(coordinates.database,
                        ".drop table %s ifexists".formatted(coordinates.table + "_strimzi"));
                LOGGER.info("Finished table clean up. Dropped table {}", coordinates.table + "_strimzi");
            } catch (Exception e) {
                LOGGER.error("Failed to clean up", e);
            }
        }
    }

    private void deployConnector() {
        Map<String, Object> connectorProps = new HashMap<>();
        connectorProps.put("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector");
        connectorProps.put("flush.interval.ms", 10000);
        connectorProps.put("tasks.max", 1);
        connectorProps.put("topics", TEST_TOPIC);
        String topicTableMapping = String.format(
                "[{'topic': '%s','db': '%s', 'table': '%s_strimzi','format':'json'}]",
                TEST_TOPIC, coordinates.database, coordinates.table);
        connectorProps.put("kusto.tables.topics.mapping", topicTableMapping);
        connectorProps.put("aad.auth.authority", coordinates.authority);
        connectorProps.put("aad.auth.accesstoken", coordinates.accessToken);
        connectorProps.put("aad.auth.strategy", "az_dev_token");
        connectorProps.put("kusto.query.url", coordinates.cluster);
        connectorProps.put("kusto.ingestion.url", coordinates.ingestCluster);
        connectorProps.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorProps.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        String connectorName = "strimzi-kusto-connector";
        kcHelper.registerConnector(connectorName, connectorProps);
        LOGGER.info("Deployed connector on Strimzi");
        kcHelper.waitUntilConnectorTaskStateChanges(connectorName, 0, "RUNNING");
        LOGGER.info("Connector state: {}", kcHelper.getConnectorTaskState(connectorName, 0));
    }

    @Test
    void shouldHandleJsonEventsWithStrimzi() throws Exception {
        LOGGER.info("Running Strimzi test for JSON format");
        Assumptions.assumeTrue(coordinates.isValidConfig(), "Skipping test due to missing configuration");
        deployConnector();
        int maxRecords = 10;
        Map<Long, String> expectedRecordsProduced = produceJsonMessages();
        performDataAssertions(maxRecords, expectedRecordsProduced);
    }

    private @NotNull Map<Long, String> produceJsonMessages() throws IOException, InterruptedException {
        LOGGER.info("-------------------Producing JSON messages to Strimzi Kafka-----------------------------");
        ContainerState kafka = COMPOSE_CONTAINER.getContainerByServiceName(KAFKA_SERVICE).
                orElseThrow();
        kafka.copyFileToContainer(MountableFile.
                        forClasspathResource("produce-test-messages.sh", 0777), // rwx--r--r--
                "/tmp/produce-test-messages.sh");
        org.testcontainers.containers.Container.ExecResult execResult = kafka.execInContainer("sh", "-c", "/tmp/produce-test-messages.sh");
        String logs = execResult.getStdout() + execResult.getStderr();
        LOGGER.info("Logs from message production:\n{}", logs);
        // The script produces 10 messages with ids from 1 to 10
        Map<Long, String> expectedRecordsProduced = new HashMap<>();
        for (long i = 1; i <= 10; i++) {
            expectedRecordsProduced.put(i, "test" + i);
        }
        return expectedRecordsProduced;
    }

    private void performDataAssertions(int maxRecords, Map<Long, String> expectedRecordsProduced) {
        String query = String.format("%s_strimzi | project id, name = pack_all()", coordinates.table);
        Map<Object, String> actualRecordsIngested = getRecordsIngested(query, maxRecords);

        actualRecordsIngested.keySet().stream()
                .filter(key -> !Objects.isNull(key) && !key.toString().trim().isEmpty())
                .forEach(key -> {
                    LOGGER.info("Record queried in assertion : {}", actualRecordsIngested.get(key));
                    long keyLong = Long.parseLong(key.toString());
                    try {
                        JSONAssert.assertEquals(expectedRecordsProduced.get(keyLong), actualRecordsIngested.get(key),
                                new CustomComparator(LENIENT,
                                        new Customization("value",
                                                (v1, v2) -> Math.abs(Double.parseDouble(v1.toString())
                                                        - Double.parseDouble(v2.toString())) < 0.0001)));
                    } catch (JSONException e) {
                        fail(e);
                    }
                });
    }

    private @NotNull Map<Object, String> getRecordsIngested(String query, int maxRecords) {
        Predicate<Object> predicate = results -> {
            if (results != null && !((Map<?, ?>) results).isEmpty()) {
                LOGGER.info("Retrieved records count {}", ((Map<?, ?>) results).size());
            }
            return results == null || ((Map<?, ?>) results).isEmpty() || ((Map<?, ?>) results).size() < maxRecords;
        };

        // Waits 30 seconds for the records to be ingested. Repeats the poll 5 times, in all 150 seconds
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(5)
                .retryOnResult(predicate)
                .waitDuration(Duration.of(30, SECONDS))
                .build();
        RetryRegistry registry = RetryRegistry.of(config);
        Retry retry = registry.retry("ingestRecordServiceStrimzi", config);

        Supplier<Map<Object, String>> recordSearchSupplier = () -> {
            try {
                LOGGER.debug("Executing query: {}", query);
                KustoResultSetTable resultSet = engineClient.executeQuery(coordinates.database, query)
                        .getPrimaryResults();
                Map<Object, String> actualResults = new HashMap<>();
                while (resultSet.next()) {
                    Object keyObject = resultSet.getObject(KEY_COLUMN);
                    Object key = (keyObject instanceof Number)
                            ? Long.parseLong(keyObject.toString())
                            : (keyObject == null ? "" : keyObject.toString());
                    String vResult = resultSet.getString("vresult");
                    LOGGER.debug("Record queried from DB: {}", vResult);
                    actualResults.put(key, vResult);
                }
                return actualResults;
            } catch (DataServiceException | DataClientException e) {
                LOGGER.warn("Query failed, will retry: {}", e.getMessage());
                return Collections.emptyMap();
            }
        };
        return retry.executeSupplier(recordSearchSupplier);
    }
}
