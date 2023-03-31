package com.microsoft.azure.kusto.kafka.connect.sink.it;

import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.kafka.connect.sink.FileWriterTest;
import com.microsoft.azure.kusto.kafka.connect.sink.Version;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

public class KustoSinkIT {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.5"))
            .withNetwork(network);

    private static final DebeziumContainer connectContainer = new DebeziumContainer("debezium/connect-base:2.2")
            .withEnv("CONNECT_PLUGIN_PATH", "/kafka/connect")
            .withFileSystemBind("target/kafka-sink-azure-kusto", "/kafka/connect/kafka-sink-azure-kusto")
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer);

    @BeforeAll
    public static void startContainers() throws Exception {
        log.info("Starting containers");
        createConnectorJar();
        Startables.deepStart(Stream.of(kafkaContainer, connectContainer)).join();
        log.info("Started containers");
    }

    private static void createConnectorJar() throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        Files.createDirectories(Paths.get("target/kafka-sink-azure-kusto"));
        try (OutputStream fos = Files.newOutputStream(
                Paths.get("target/kafka-sink-azure-kusto/kafka-sink-azure-kusto.jar"));
                JarOutputStream target = new JarOutputStream(fos, manifest)) {
            add(new File("target/classes"), target);
        }
    }

    private static void createTables() throws Exception {
//        String table = tableBaseName + dataFormat;
//        String mappingReference = dataFormat + "Mapping";
//        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
//                String.format("https://%s.kusto.windows.net/", cluster),
//                appId, appKey, authority);
//        Client engineClient = ClientFactory.createClient(engineCsb);
//        String basepath = Paths.get(basePath, dataFormat).toString();
//        try {
//            if (tableBaseName.startsWith(testPrefix)) {
//                engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
//                engineClient.execute(database, String.format(
//                        ".alter table %s policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:05\", \"MaximumNumberOfItems\": 2, \"MaximumRawDataSizeMB\": 100}'",
//                        table));
//                if (streaming) {
//                    engineClient.execute(database, ".clear database cache streamingingestion schema");
//                }
//            }
//            engineClient.execute(database, String.format(".create table ['%s'] ingestion %s mapping '%s' " +
//                    "'[" + mapping + "]'", table, dataFormat, mappingReference));
    }

    private static void add(File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/").replace("target/classes/", "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : Objects.requireNonNull(source.listFiles())) {
                add(nestedFile, target);
            }
        } else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(source.toPath()))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1) {
                        break;
                    }
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }

    @Test
    public void shouldHandleAllTypesOfEvents() throws Exception {
        log.info("Deploying connector");
        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector")
                .with("flush.size.bytes", 10000)
                .with("flush.interval.ms", 1000)
                .with("tasks.max", 1)
                .with("topics", "multijson.topic")
                .with("kusto.tables.topics.mapping", "[{'topic': 'multijson.topic','db': 'sdktestsdb', 'table': 'MultiJson','format':'json'}]")
                .with("aad.auth.authority", "https://login.microsoftonline.com/")
                .with("aad.auth.appid", "")
                .with("aad.auth.appkey", "")
                .with("kusto.ingestion.url", "https://ingest-x.eastus.dev.kusto.windows.net")
                .with("kusto.query.url", "https://x.eastus.dev.kusto.windows.net")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectContainer.registerConnector("adx-connector", connector);
        log.info("Deployed connector");
        log.info(connectContainer.getLogs());
        connectContainer.ensureConnectorTaskState("adx-connector", 0, Connector.State.RUNNING);
        log.info(connectContainer.getConnectorTaskState("adx-connector", 0).name());
    }

    private void produceKafkaMessages() {
        log.info("Producing messages");
        Map<String,Object> producerProperties = new HashMap<>();
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
        // Make the paths to be safe. Remove any null paths
        return sanitize ? FilenameUtils.normalizeNoEndSeparator(value) : value;
    }

    @BeforeAll
    public static void beforeAll() {
        String testPrefix = "tmpKafkaE2ETest";
        String appId = getProperty("appId", "", false);
        String appKey = getProperty("appKey", "", false);
        String authority = getProperty("authority", "", false);
        String cluster = getProperty("cluster", "", false);
        String database = getProperty("database", "e2e", true);
        String defaultTable = testPrefix + UUID.randomUUID().toString().replace('-', '_');
        String table = getProperty("table", defaultTable, true);

    }
}
