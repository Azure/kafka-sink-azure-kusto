package com.microsoft.azure.kusto.kafka.connect.sink.it;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.*;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.kafka.connect.sink.Version;
import com.microsoft.azure.kusto.kafka.connect.sink.it.containers.KustoKafkaConnectContainer;
import com.microsoft.azure.kusto.kafka.connect.sink.it.containers.ProxyContainer;
import com.microsoft.azure.kusto.kafka.connect.sink.it.containers.SchemaRegistryContainer;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.getConnectorProperties;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

class KustoSinkIT {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String confluentVersion = "6.2.5";
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + confluentVersion))
            .withNetwork(network);
    private static final ProxyContainer proxyContainer = new ProxyContainer().withNetwork(network);
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(confluentVersion).withKafka(kafkaContainer)
            .withNetwork(network).dependsOn(kafkaContainer);
    private static final List<String> testFormats = Arrays.asList("json", "avro", "csv"); // List.of("json", "avro", "csv", "raw"); // Raw for XML
    private static ITCoordinates coordinates;
    private static final KustoKafkaConnectContainer connectContainer = new KustoKafkaConnectContainer(confluentVersion)
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer, proxyContainer, schemaRegistryContainer);

    private static Client engineClient = null;
    private static Client dmClient = null;

    private static final String keyColumn = "vlong";

    @BeforeAll
    public static void startContainers() throws Exception {
        coordinates = getConnectorProperties();
        if (coordinates.isValidConfig()) {
            ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(coordinates.cluster,
                    coordinates.accessToken);
            ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(coordinates.ingestCluster, coordinates.accessToken);
            engineClient = ClientFactory.createClient(engineCsb);
            dmClient = ClientFactory.createClient(dmCsb);
            log.info("Creating tables in Kusto");
            createTables();
            refreshDm();
            // Mount the libs
            String mountPath = String.format(
                    "target/components/packages/microsoftcorporation-kafka-sink-azure-kusto-%s/microsoftcorporation-kafka-sink-azure-kusto-%s/lib",
                    Version.getVersion(), Version.getVersion());
            log.info("Creating connector jar with version {} and mounting it from {},", Version.getVersion(), mountPath);
            connectContainer.withFileSystemBind(mountPath, "/kafka/connect/kafka-sink-azure-kusto");
            Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer, proxyContainer, connectContainer)).join();
            log.info("Started containers , copying scripts to container and executing them");
            connectContainer.withCopyToContainer(MountableFile.forClasspathResource("download-libs.sh", 744), // rwx--r--r--
                    "/kafka/connect/kafka-sink-azure-kusto/download-libs.sh").execInContainer("sh", "/kafka/connect/kafka-sink-azure-kusto/download-libs.sh");
            // Logs of start up of the container gets published here. This will be handy in case we want to look at startup failures
            log.debug(connectContainer.getLogs());
        } else {
            log.info("Skipping test due to missing configuration");
        }
    }

    private static void createTables() throws Exception {
        URL kqlResource = KustoSinkIT.class.getClassLoader().getResource("it-table-setup.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream().map(kql -> kql.replace("TBL", coordinates.table))
                .collect(Collectors.toList());
        kqlsToExecute.forEach(kql -> {
            try {
                engineClient.execute(coordinates.database, kql);
            } catch (Exception e) {
                log.error("Failed to execute kql: {}", kql, e);
            }
        });
        log.info("Created table {} and associated mappings", coordinates.table);
    }

    private static void refreshDm() throws Exception {
        URL kqlResource = KustoSinkIT.class.getClassLoader().getResource("dm-refresh-cache.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream().map(kql -> kql.replace("TBL", coordinates.table))
                .map(kql -> kql.replace("DB", coordinates.database))
                .collect(Collectors.toList());
        kqlsToExecute.forEach(kql -> {
            try {
                dmClient.execute(kql);
            } catch (Exception e) {
                log.error("Failed to execute DM kql: {}", kql, e);
            }
        });
        log.info("Refreshed cache on DB {}", coordinates.database);
    }

    @AfterAll
    public static void stopContainers() throws Exception {
        connectContainer.stop();
        schemaRegistryContainer.stop();
        kafkaContainer.stop();
        engineClient.execute(coordinates.database, String.format(".drop table %s", coordinates.table));
        log.warn("Finished table clean up. Dropped table {}", coordinates.table);
        dmClient.close();
        engineClient.close();
    }

    @Test
    public void shouldHandleAllTypesOfEvents() {
        Assumptions.assumeTrue(coordinates.isValidConfig(), "Skipping test due to missing configuration");
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        testFormats.parallelStream().forEach(dataFormat -> {
            String valueFormat = "org.apache.kafka.connect.storage.StringConverter";
            if (dataFormat.equals("avro")) {
                valueFormat = AvroConverter.class.getName();
                log.debug("Using value format: {}", valueFormat);
            }
            String topicTableMapping = dataFormat.equals("csv")
                    ? String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'%s_mapping','streaming':'true'}]", dataFormat,
                            coordinates.database,
                            coordinates.table, dataFormat, dataFormat)
                    : String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'%s_mapping'}]", dataFormat,
                            coordinates.database,
                            coordinates.table, dataFormat, dataFormat);
            log.info("Deploying connector for {} , using SR url {}. Using proxy host {} and port {}", dataFormat, srUrl,
                    proxyContainer.getContainerId().substring(0, 12), proxyContainer.getExposedPorts().get(0));
            Map<String, Object> connectorProps = new HashMap<>();
            connectorProps.put("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector");
            connectorProps.put("flush.size.bytes", 10000);
            connectorProps.put("flush.interval.ms", 1000);
            connectorProps.put("tasks.max", 1);
            connectorProps.put("topics", String.format("e2e.%s.topic", dataFormat));
            connectorProps.put("kusto.tables.topics.mapping", topicTableMapping);
            connectorProps.put("aad.auth.authority", coordinates.authority);
            connectorProps.put("aad.auth.accesstoken", coordinates.accessToken);
            connectorProps.put("aad.auth.strategy", "AZ_DEV_TOKEN".toLowerCase());
            connectorProps.put("kusto.query.url", coordinates.cluster);
            connectorProps.put("kusto.ingestion.url", coordinates.ingestCluster);
            connectorProps.put("schema.registry.url", srUrl);
            connectorProps.put("value.converter.schema.registry.url", srUrl);
            connectorProps.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
            connectorProps.put("value.converter", valueFormat);
            connectorProps.put("proxy.host", proxyContainer.getContainerId().substring(0, 12));
            connectorProps.put("proxy.port", proxyContainer.getExposedPorts().get(0));
            connectContainer.registerConnector(String.format("adx-connector-%s", dataFormat), connectorProps);
            log.info("Deployed connector for {}", dataFormat);
            log.debug(connectContainer.getLogs());
        });
        testFormats.parallelStream().forEach(dataFormat -> {
            connectContainer.waitUntilConnectorTaskStateChanges(String.format("adx-connector-%s", dataFormat), 0, "RUNNING");
            log.info("Connector state for {} : {}. ", dataFormat,
                    connectContainer.getConnectorTaskState(String.format("adx-connector-%s", dataFormat), 0));
            try {
                produceKafkaMessages(dataFormat);
                Thread.sleep(10000);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void produceKafkaMessages(@NotNull String dataFormat) throws IOException {
        log.debug("Producing messages");
        int maxRecords = 10;
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                String.format("http://%s:%s", schemaRegistryContainer.getHost(), schemaRegistryContainer.getFirstMappedPort()));
        producerProperties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        // avro
        Generator.Builder builder = new Generator.Builder().schemaString(IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("it-avro.avsc")),
                StandardCharsets.UTF_8));
        Generator randomDataBuilder = builder.build();
        Map<Long, String> expectedRecordsProduced = new HashMap<>();

        switch (dataFormat) {
            case "avro":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                // GenericRecords to bytes using avro
                try (KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericData.Record record = (GenericData.Record) randomDataBuilder.generate();
                        ProducerRecord<String, GenericData.Record> producerRecord = new ProducerRecord<>("e2e.avro.topic", "Key-" + i, record);
                        Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                        jsonRecordMap.put("type", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
                    }
                }
                break;
            case "json":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                // GenericRecords to json using avro
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                        Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("e2e.json.topic", "Key-" + i,
                                objectMapper.writeValueAsString(jsonRecordMap));
                        jsonRecordMap.put("type", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
                        log.debug("JSON Record produced: {}", objectMapper.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
                    }
                }
                break;
            case "csv":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                // GenericRecords to json using avro
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                        Map<String, Object> jsonRecordMap = new TreeMap<>(record.getSchema().getFields().stream().parallel()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name()))));
                        String objectsCommaSeparated = jsonRecordMap.values().stream().map(Object::toString).collect(Collectors.joining(","));
                        log.debug("CSV Record produced: {}", objectsCommaSeparated);
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("e2e.csv.topic", "Key-" + i,
                                objectsCommaSeparated);
                        jsonRecordMap.put("type", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
                    }
                }
                break;
        }
        log.info("Produced messages for format {}", dataFormat);
        Map<Long, String> actualRecordsIngested = getRecordsIngested(dataFormat, maxRecords);
        actualRecordsIngested.keySet().parallelStream().forEach(key -> {
            log.debug("Record queried in assertion : {}", actualRecordsIngested.get(key));
            try {
                JSONAssert.assertEquals(expectedRecordsProduced.get(key), actualRecordsIngested.get(key),
                        new CustomComparator(LENIENT,
                                // there are sometimes round off errors in the double values but they are close enough to 8 precision
                                new Customization("vdec", (vdec1,
                                        vdec2) -> Math.abs(Double.parseDouble(vdec1.toString()) - Double.parseDouble(vdec2.toString())) < 0.000000001),
                                new Customization("vreal", (vreal1,
                                        vreal2) -> Math.abs(Double.parseDouble(vreal1.toString()) - Double.parseDouble(vreal2.toString())) < 0.0001)));
            } catch (JSONException e) {
                fail(e);
            }
        });
        assertEquals(maxRecords, actualRecordsIngested.size());
    }

    private @NotNull Map<Long, String> getRecordsIngested(String dataFormat, int maxRecords) {
        String query = String.format("%s | where type == '%s' | project  %s,vresult = pack_all()", coordinates.table, dataFormat, keyColumn);
        Predicate<Object> predicate = (results) -> {
            if (results != null) {
                log.debug("Retrieved records count {}", ((Map<?, ?>) results).size());
            }
            return results == null || ((Map<?, ?>) results).isEmpty() || ((Map<?, ?>) results).size() < maxRecords;
        };
        // Waits 30 seconds for the records to be ingested. Repeats the poll 5 times , in all 150 seconds
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(5)
                .retryOnResult(predicate)
                .waitDuration(Duration.of(30, SECONDS))
                .build();
        RetryRegistry registry = RetryRegistry.of(config);
        Retry retry = registry.retry("ingestRecordService", config);
        Supplier<Map<Long, String>> recordSearchSupplier = () -> {
            try {
                log.debug("Executing query {} ", query);
                KustoResultSetTable resultSet = engineClient.execute(coordinates.database, query).getPrimaryResults();
                Map<Long, String> actualResults = new HashMap<>();
                while (resultSet.next()) {
                    Long key = (long) resultSet.getInt(keyColumn);
                    String vResult = resultSet.getString("vresult");
                    log.debug("Record queried: {}", vResult);
                    actualResults.put(key, vResult);
                }
                return actualResults;
            } catch (DataServiceException | DataClientException e) {
                return Collections.emptyMap();
            }
        };
        return retry.executeSupplier(recordSearchSupplier);
    }
}
