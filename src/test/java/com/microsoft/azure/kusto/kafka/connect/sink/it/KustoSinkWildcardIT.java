package com.microsoft.azure.kusto.kafka.connect.sink.it;

import static com.microsoft.azure.kusto.kafka.connect.sink.Utils.getConnectProperties;
import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.*;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.fail;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.kafka.connect.sink.ListUtils;
import com.microsoft.azure.kusto.kafka.connect.sink.Utils;
import com.microsoft.azure.kusto.kafka.connect.sink.Version;
import com.microsoft.azure.kusto.kafka.connect.sink.it.containers.KustoKafkaConnectContainerHelper;
import com.microsoft.azure.kusto.kafka.connect.sink.it.containers.ProxyContainer;
import io.confluent.avro.random.generator.Generator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class KustoSinkWildcardIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(KustoSinkWildcardIT.class);
    private static final String KEY_COLUMN = "vlong";

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
            .withListener(LISTENER_ADDRESS)
            .withNetwork(NETWORK).withCreateContainerCmdModifier(cmd -> cmd.withName("kafka-wildcard"));
    
    @Container
    private static final ProxyContainer PROXY = new ProxyContainer().withNetwork(NETWORK);
    
    @Container
    private static final GenericContainer<?> KAFKA_CONNECT = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-kafka-connect-base:" + CONFLUENT_VERSION))
            .withNetwork(NETWORK)
            .withExposedPorts(KAFKA_CONNECT_PORT)
            .withNetworkAliases("kafka-connect-wildcard")
            .withCopyToContainer(MountableFile.forHostPath(Path.of("target/kafka-sink-azure-kusto-%s-jar-with-dependencies.jar".formatted(
                    Version.getVersion()))), Utils.getConnectPath())
            .withCreateContainerCmdModifier(cmd -> cmd.withName("kafka-connect-wildcard"))
            .withEnv(getConnectProperties())
            .dependsOn(KAFKA, PROXY);
            
    private final KustoKafkaConnectContainerHelper kcHelper = new KustoKafkaConnectContainerHelper(KAFKA_CONNECT);

    private static ITCoordinates coordinates;
    private static Client engineClient = null;
    private static Client dmClient = null;

    @BeforeAll
    public static void startContainers() throws Exception {
        try {
            coordinates = getConnectorProperties();
        } catch (Exception e) {
            LOGGER.info("Skipping test due to error while fetching connector properties: {}", e.getMessage());
            return;
        }
        
        if (coordinates.isValidConfig()) {
            ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(coordinates.cluster,
                    coordinates.accessToken);
            ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(coordinates.ingestCluster, coordinates.accessToken);
            engineClient = ClientFactory.createClient(engineCsb);
            dmClient = ClientFactory.createClient(dmCsb);
            createTables();
            refreshDm();
            
            KAFKA_CONNECT.withCopyToContainer(MountableFile.forClasspathResource("download-libs.sh", 744),
                    "%s/download-libs.sh".formatted(Utils.getConnectPath()))
                    .execInContainer("sh", "%s/download-libs.sh".formatted(Utils.getConnectPath()));
        } else {
            LOGGER.info("Skipping test due to missing configuration");
        }
    }

    private static void createTables() throws Exception {
        URL kqlResource = KustoSinkWildcardIT.class.getClassLoader().getResource("it-table-setup.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Path.of(kqlResource.toURI())).stream().map(kql -> kql.replace("TBL", coordinates.table))
                .toList();
        kqlsToExecute.forEach(kql -> {
            try {
                if (kql.startsWith(".")) {
                    engineClient.executeMgmt(coordinates.database, kql);
                } else {
                    engineClient.executeQuery(coordinates.database, kql);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to execute kql: {}", kql, e);
            }
        });
    }

    private static void refreshDm() throws Exception {
        URL kqlResource = KustoSinkWildcardIT.class.getClassLoader().getResource("dm-refresh-cache.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Path.of(kqlResource.toURI())).stream().map(kql -> kql.replace("TBL", coordinates.table))
                .map(kql -> kql.replace("DB", coordinates.database))
                .toList();
        kqlsToExecute.forEach(kql -> {
            try {
                dmClient.executeMgmt(kql);
            } catch (Exception e) {
                LOGGER.error("Failed to execute DM kql: {}", kql, e);
            }
        });
    }

    @AfterAll
    public static void stopContainers() throws InterruptedException {
        KAFKA_CONNECT.stop();
        KAFKA.stop();
        if (coordinates != null && engineClient != null) {
            try {
                engineClient.executeMgmt(coordinates.database, ".drop table %s".formatted(coordinates.table));
            } catch (Exception e) {
                LOGGER.error("Failed to drop table", e);
            }
        }
    }

    @Test
    void shouldHandleWildcardSubscription() throws IOException {
        Assumptions.assumeTrue(coordinates != null && coordinates.isValidConfig(), "Skipping test due to missing configuration");
        
        String dataFormat = "json";
        String wildcardTopic = "wildcard.*";
        String topic1 = "wildcard.topic1";
        String topic2 = "wildcard.topic2";
        
        // Topic mapping using wildcard '*'
        String topicTableMapping = "[{'topic': '*','db': '%s', 'table': '%s','format':'%s','mapping':'data_mapping'}]"
                .formatted(coordinates.database, coordinates.table, dataFormat);

        Map<String, Object> connectorProps = new HashMap<>();
        connectorProps.put("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector");
        connectorProps.put("flush.size.bytes", 1000);
        connectorProps.put("flush.interval.ms", 1000);
        connectorProps.put("tasks.max", 1);
        connectorProps.put("topics.regex", wildcardTopic);
        connectorProps.put("kusto.tables.topics.mapping", topicTableMapping);
        connectorProps.put("aad.auth.authority", coordinates.authority);
        connectorProps.put("aad.auth.accesstoken", coordinates.accessToken);
        connectorProps.put("aad.auth.strategy", "AZ_DEV_TOKEN".toLowerCase());
        connectorProps.put("kusto.query.url", coordinates.cluster);
        connectorProps.put("kusto.ingestion.url", coordinates.ingestCluster);
        connectorProps.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorProps.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorProps.put("proxy.host", PROXY.getContainerId().substring(0, 12));
        connectorProps.put("proxy.port", ListUtils.getFirst(PROXY.getExposedPorts()));

        String connectorName = "adx-connector-wildcard";
        kcHelper.registerConnector(connectorName, connectorProps);
        kcHelper.waitUntilConnectorTaskStateChanges(connectorName, 0, "RUNNING");

        int recordsPerTopic = 5;
        Map<Long, String> expectedRecordsProduced = new HashMap<>();
        
        expectedRecordsProduced.putAll(produceKafkaMessages(topic1, recordsPerTopic));
        expectedRecordsProduced.putAll(produceKafkaMessages(topic2, recordsPerTopic));

        performDataAssertions(recordsPerTopic * 2, expectedRecordsProduced);
    }

    private Map<Long, String> produceKafkaMessages(String topic, int maxRecords) throws IOException {
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Generator.Builder builder = new Generator.Builder().schemaString(IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("it-avro.avsc")),
                StandardCharsets.UTF_8));
        Generator randomDataBuilder = builder.build();
        Map<Long, String> expectedRecordsProduced = new HashMap<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            for (int i = 0; i < maxRecords; i++) {
                GenericRecord genericRecord = (GenericRecord) randomDataBuilder.generate();
                genericRecord.put("vtype", "json");
                Map<String, Object> jsonRecordMap = genericRecord.getSchema().getFields().stream()
                        .collect(Collectors.toMap(Schema.Field::name, field -> genericRecord.get(field.name())));
                
                String jsonValue = OBJECT_MAPPER.writeValueAsString(jsonRecordMap);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, 0, "Key-" + topic + "-" + i, jsonValue);
                
                expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(KEY_COLUMN).toString()), jsonValue);
                producer.send(producerRecord);
            }
        }
        return expectedRecordsProduced;
    }

    private void performDataAssertions(int totalRecords, Map<Long, String> expectedRecordsProduced) {
        String query = "%s | where vtype == 'json' | project %s,vresult = pack_all()".formatted(coordinates.table, KEY_COLUMN);
        Map<Object, String> actualRecordsIngested = getRecordsIngested(query, totalRecords);
        
        actualRecordsIngested.keySet().forEach(key -> {
            long keyLong = Long.parseLong(key.toString());
            try {
                JSONAssert.assertEquals(expectedRecordsProduced.get(keyLong), actualRecordsIngested.get(key),
                        new CustomComparator(LENIENT,
                                new Customization("vdec", (vdec1, vdec2) -> Math.abs(Double.parseDouble(vdec1.toString()) - Double.parseDouble(vdec2.toString())) < 0.000000001),
                                new Customization("vreal", (vreal1, vreal2) -> Math.abs(Double.parseDouble(vreal1.toString()) - Double.parseDouble(vreal2.toString())) < 0.0001)));
            } catch (JSONException e) {
                fail(e);
            }
        });
    }

    private Map<Object, String> getRecordsIngested(String query, int maxRecords) {
        Predicate<Object> predicate = results -> results == null || ((Map<?, ?>) results).size() < maxRecords;
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(10)
                .retryOnResult(predicate)
                .waitDuration(Duration.of(30, SECONDS))
                .build();
        RetryRegistry registry = RetryRegistry.of(config);
        Retry retry = registry.retry("ingestRecordService", config);
        
        Supplier<Map<Object, String>> recordSearchSupplier = () -> {
            try {
                KustoResultSetTable resultSet = engineClient.executeQuery(coordinates.database, query).getPrimaryResults();
                Map<Object, String> actualResults = new HashMap<>();
                while (resultSet.next()) {
                    Object keyObject = resultSet.getObject(KEY_COLUMN);
                    Object key = (keyObject instanceof Number) ? Long.parseLong(keyObject.toString()) : keyObject.toString();
                    actualResults.put(key, resultSet.getString("vresult"));
                }
                return actualResults;
            } catch (Exception e) {
                return Collections.emptyMap();
            }
        };
        return retry.executeSupplier(recordSearchSupplier);
    }
}
