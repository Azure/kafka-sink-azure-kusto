package com.microsoft.azure.kusto.kafka.connect.sink.it;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.createConnectorJar;
import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.getConnectorProperties;

public class KustoSinkIT {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.5"))
            .withNetwork(network);
    private static final List<String> testFormats = List.of("json", "avro", "csv", "raw"); // Raw for XML

    private static ITCoordinates coordinates;
    private static final DebeziumContainer connectContainer = new DebeziumContainer("debezium/connect-base:2.2")
            .withEnv("CONNECT_PLUGIN_PATH", "/kafka/connect")
            .withFileSystemBind("target/kafka-sink-azure-kusto", "/kafka/connect/kafka-sink-azure-kusto")
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer);

    @BeforeAll
    public static void startContainers() throws Exception {
        coordinates = getConnectorProperties();
        if (coordinates.isValidConfig()) {
            log.info("Creating connector jar");
            createConnectorJar();
            log.info("Starting containers");
            Startables.deepStart(Stream.of(kafkaContainer, connectContainer)).join();
            log.info("Started containers");
        } else {
            log.info("Skipping test due to missing configuration");
        }
    }

    private static void createTables() throws Exception {
        // String table = tableBaseName + dataFormat;
        // String mappingReference = dataFormat + "Mapping";
        // ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
        // String.format("https://%s.kusto.windows.net/", cluster),
        // appId, appKey, authority);
        // Client engineClient = ClientFactory.createClient(engineCsb);
        // String basepath = Paths.get(basePath, dataFormat).toString();
        // try {
        // if (tableBaseName.startsWith(testPrefix)) {
        // engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
        // engineClient.execute(database, String.format(
        // ".alter table %s policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:05\", \"MaximumNumberOfItems\": 2, \"MaximumRawDataSizeMB\": 100}'",
        // table));
        // if (streaming) {
        // engineClient.execute(database, ".clear database cache streamingingestion schema");
        // }
        // }
        // engineClient.execute(database, String.format(".create table ['%s'] ingestion %s mapping '%s' " +
        // "'[" + mapping + "]'", table, dataFormat, mappingReference));
    }

    @Test
    public void shouldHandleAllTypesOfEvents() throws Exception {
        Assumptions.assumeTrue(coordinates.isValidConfig(), "Skipping test due to missing configuration");
        testFormats.forEach(dataFormat -> {
            log.info("Deploying connector for {}", dataFormat);
            ConnectorConfiguration connector = ConnectorConfiguration.create()
                    .with("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector")
                    .with("flush.size.bytes", 10000)
                    .with("flush.interval.ms", 1000)
                    .with("tasks.max", 1)
                    .with("topics", String.format("e2e.%s.topic", dataFormat))
                    .with("kusto.tables.topics.mapping",
                            String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s'}]", dataFormat, coordinates.database,
                                    coordinates.table, dataFormat))
                    .with("aad.auth.authority", coordinates.authority)
                    .with("aad.auth.appid", coordinates.appId)
                    .with("aad.auth.appkey", coordinates.appKey)
                    .with("kusto.ingestion.url", String.format("https://ingest-%s.kusto.windows.net", coordinates.cluster))
                    .with("kusto.query.url", String.format("https://%s.kusto.windows.net", coordinates.cluster))
                    .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");
            connectContainer.registerConnector(String.format("adx-connector-%s", dataFormat), connector);
            log.info("Deployed connector for {}", dataFormat);
            log.info(connectContainer.getLogs());
        });
        testFormats.forEach(dataFormat -> {
            connectContainer.ensureConnectorTaskState(String.format("adx-connector-%s", dataFormat), 0, Connector.State.RUNNING);
            log.info("Connector state for {} : {}. ", dataFormat,
                    connectContainer.getConnectorTaskState(String.format("adx-connector-%s", dataFormat), 0).name());
        });
    }

    private void produceKafkaMessages() {
        log.info("Producing messages");
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("multijson.topic", "key", "{\"id\": " + i + ", \"name\": \"name" + i + "\"}"));
            }
        }
        log.info("Produced messages");
    }

    private static String getProperty(String attribute, String defaultValue, boolean sanitize) {
        String value = System.getProperty(attribute, defaultValue);
        return sanitize ? FilenameUtils.normalizeNoEndSeparator(value) : value;
    }

}
