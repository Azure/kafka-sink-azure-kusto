package com.microsoft.azure.kusto.kafka.connect.sink.it;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.github.resilience4j.retry.RetryConfig;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.createConnectorJar;
import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.getConnectorProperties;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KustoSinkIT {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String confluentVersion = "6.2.5";
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + confluentVersion))
            .withNetwork(network);
    private static final List<String> testFormats = List.of("avro"); // List.of("json", "avro", "csv", "raw"); // Raw for XML

    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ITCoordinates coordinates;
    private static final DebeziumContainer connectContainer = new DebeziumContainer("debezium/connect-base:2.2")
            .withEnv("CONNECT_PLUGIN_PATH", "/kafka/connect")
            .withFileSystemBind("target/kafka-sink-azure-kusto", "/kafka/connect/kafka-sink-azure-kusto")
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer);

    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(confluentVersion);

    private static Client engineClient = null;

    @BeforeAll
    public static void startContainers() throws Exception {
        coordinates = getConnectorProperties();
        if (coordinates.isValidConfig()) {
            ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    String.format("https://%s.kusto.windows.net/", coordinates.cluster),
                    coordinates.appId, coordinates.appKey, coordinates.authority);
            engineClient = ClientFactory.createClient(engineCsb);
            log.info("Creating tables in Kusto");
            createTables();
            log.info("Creating connector jar");
            createConnectorJar();
            log.info("Starting containers");
            Startables.deepStart(Stream.of(kafkaContainer, connectContainer)).join();
            schemaRegistryContainer.withKafka(kafkaContainer).withNetwork(network).start();
            log.info("Started containers");
        } else {
            log.info("Skipping test due to missing configuration");
        }
    }

    private static void createTables() throws Exception {
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                String.format("https://%s.kusto.windows.net/", coordinates.cluster),
                coordinates.appId, coordinates.appKey, coordinates.authority);
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
    }

    @AfterAll
    public static void stopContainers() throws Exception {
        connectContainer.stop();
        schemaRegistryContainer.stop();
        kafkaContainer.stop();
        engineClient.execute(coordinates.database, String.format(".drop table %s", coordinates.table));
        log.error("Finished table clean up. Dropped table {}", coordinates.table);
    }

    @Test
    public void shouldHandleAllTypesOfEvents() throws Exception {
        Assumptions.assumeTrue(coordinates.isValidConfig(), "Skipping test due to missing configuration");
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        testFormats.forEach(dataFormat -> {
            String valueFormat = "org.apache.kafka.connect.storage.StringConverter";
            if (dataFormat.equals("avro")) {
                valueFormat = AvroConverter.class.getName();

                log.error("Using value format: {}", valueFormat);
            }
            log.info("Deploying connector for {} , using SR url {}", dataFormat, srUrl);
            ConnectorConfiguration connector = ConnectorConfiguration.create()
                    .with("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector")
                    .with("flush.size.bytes", 10000)
                    .with("flush.interval.ms", 1000)
                    .with("tasks.max", 1)
                    .with("topics", String.format("e2e.%s.topic", dataFormat))
                    .with("kusto.tables.topics.mapping",
                            String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'%s_mapping'}]", dataFormat,
                                    coordinates.database,
                                    coordinates.table, dataFormat, dataFormat))
                    .with("aad.auth.authority", coordinates.authority)
                    .with("aad.auth.appid", coordinates.appId)
                    .with("aad.auth.appkey", coordinates.appKey)
                    .with("kusto.ingestion.url", String.format("https://ingest-%s.kusto.windows.net", coordinates.cluster))
                    .with("kusto.query.url", String.format("https://%s.kusto.windows.net", coordinates.cluster))
                    .with("schema.registry.url", srUrl)
                    .with("value.converter.schema.registry.url", srUrl)
                    .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .with("value.converter", valueFormat);
            connectContainer.registerConnector(String.format("adx-connector-%s", dataFormat), connector);
            log.info("Deployed connector for {}", dataFormat);
            log.info(connectContainer.getLogs());
        });
        testFormats.forEach(dataFormat -> {
            connectContainer.ensureConnectorTaskState(String.format("adx-connector-%s", dataFormat), 0, Connector.State.RUNNING);
            log.info("Connector state for {} : {}. ", dataFormat,
                    connectContainer.getConnectorTaskState(String.format("adx-connector-%s", dataFormat), 0).name());
            try {
                produceKafkaMessages(dataFormat);
                Thread.sleep(10000);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void produceKafkaMessages(String dataFormat) throws IOException {
        log.info("Producing messages");
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

        if (dataFormat.equals("avro")) {
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            // GenericRecords to bytes using avro
            try (KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<>(producerProperties)) {
                Map<String,String> recordsProduced = new HashMap<>();
                for (int i = 0; i < 10; i++) {
                    GenericData.Record record = (GenericData.Record) randomDataBuilder.generate();
                    ProducerRecord<String, GenericData.Record> producerRecord = new ProducerRecord<>("e2e.avro.topic", "Key-" + i, record);
                    Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                            .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                    recordsProduced.put(jsonRecordMap.get("vstr").toString(),objectMapper.writeValueAsString(jsonRecordMap));
                    producer.send(producerRecord);
                    Map<String,String> actualRecordsIngested = getRecordsIngested(dataFormat);
                    assertEquals(recordsProduced,actualRecordsIngested);
                }
            }
        } else if (dataFormat.equals("json")) {
            Map<String,String> recordsProduced = new HashMap<>();
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            // GenericRecords to json using avro
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                for (int i = 0; i < 10; i++) {
                    GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                    Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                            .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("e2e.json.topic", "Key-" + i,
                            objectMapper.writeValueAsString(jsonRecordMap));
                    recordsProduced.put(jsonRecordMap.get("vstr").toString(),objectMapper.writeValueAsString(jsonRecordMap));
                    producer.send(producerRecord);
                    Map<String,String> actualRecordsIngested = getRecordsIngested(dataFormat);
                    assertEquals(recordsProduced,actualRecordsIngested);
                }
            }
        }
        log.info("Produced messages for format {}", dataFormat);
    }

    private Map<String,String> getRecordsIngested(String dataFormat) {
        // Waits 60 seconds for the records to be ingested
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(6)
                .waitDuration(Duration.of(10, SECONDS))
                .build();
        RetryRegistry registry = RetryRegistry.of(config);
        Retry retry = registry.retry("ingestRecordService", config);
        Supplier<Map<String,String>> recordSearchSupplier = () -> {
            try {
               String results =  engineClient.executeToJsonResult(coordinates.database, String.format("%s | where type == '%s' | order by vstr desc", coordinates.table,dataFormat));
               Map<String,String> actualResults = new HashMap<>();
               ArrayNode allResults =  (ArrayNode)objectMapper.readTree(results);
                allResults.forEach(node -> {
                    String key = node.get("vstr").textValue();
                    actualResults.put(key,node.toString());
                });
                return actualResults;
            } catch (DataServiceException | DataClientException | IOException e) {
                return Collections.emptyMap();
            }
        };
        return Retry.decorateSupplier(retry, recordSearchSupplier).get();
    }

    static private class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
        public static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry";
        public static final int SCHEMA_REGISTRY_PORT = 8081;

        public SchemaRegistryContainer() {
            this(confluentVersion);
        }

        public SchemaRegistryContainer(String version) {
            super(SCHEMA_REGISTRY_IMAGE + ":" + version);
            waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
            withExposedPorts(SCHEMA_REGISTRY_PORT);
        }

        public SchemaRegistryContainer withKafka(KafkaContainer kafka) {
            return withKafka(kafka.getNetwork(), kafka.getNetworkAliases().get(0) + ":9092");
        }

        public SchemaRegistryContainer withKafka(Network network, String bootstrapServers) {
            withNetwork(network).withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry").withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
            return self();
        }
    }
}
