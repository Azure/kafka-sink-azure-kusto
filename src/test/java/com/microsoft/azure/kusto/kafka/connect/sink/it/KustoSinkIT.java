package com.microsoft.azure.kusto.kafka.connect.sink.it;

import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
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
import java.util.Objects;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

public class KustoSinkIT {
    private static final Network network = Network.newNetwork();
    private static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.5"))
                    .withNetwork(network);

    private static final DebeziumContainer connectContainer = new DebeziumContainer("debezium/connect-base:1.9.5.Final")
            .withFileSystemBind("target/kafka-sink-azure-kusto", "/kafka/connect/kafka-sink-azure-kusto")
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer);

    @BeforeAll
    public static void startContainers() throws Exception {
        createConnectorJar();
        Startables.deepStart(Stream.of(kafkaContainer, connectContainer)).join();
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
    public void shouldHandleAllTypesOfEvents(String topicNamePrefix,
                                             String appId,
                                             String appKey,
                                             String authority,
                                             String databaseName,
                                             String mappingFileName,
                                             IngestionProperties.DataFormat dataFormat) throws Exception {


        

        switch (dataFormat){
            case CSV:
                break;
            case AVRO:
                break;
            case JSON:
                break;
        }

        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector")
                .with("flush.size.bytes", 10000)
                .with("flush.interval.ms", 10000)
                .with("tasks.max", 1)
                .with("topics", topicName)
                .with("kusto.tables.topics.mapping", "[{'topic': 'multijson.topic','db': 'sdktestsdb', 'table': 'MultiJson','format':'json'}]")
                .with("aad.auth.authority", authority)
                .with("aad.auth.appid",appId )
                .with("aad.auth.appkey", appKey)
                .with("kusto.ingestion.url", "https://ingest-sdke2etestcluster.eastus.dev.kusto.windows.net")
                .with("kusto.query.url", "https://sdke2etestcluster.eastus.dev.kusto.windows.net")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectContainer.registerConnector("adx-connector", connector);
        connectContainer.ensureConnectorTaskState("adx-connector", 0, Connector.State.RUNNING);
    }

    private static String getProperty(String attribute, String defaultValue, boolean sanitize){
        String value = System.getProperty(attribute,defaultValue);
        // Make the paths to be safe. Remove any null paths
        return sanitize ? FilenameUtils.normalizeNoEndSeparator(value) : value;
    }

    @BeforeAll
    public void beforeAll(){
        String testPrefix = "tmpKafkaE2ETest";
        String appId = getProperty("appId","",false);
        String appKey = getProperty("appKey","",false);
        String authority = getProperty("authority","",false);
        String cluster = getProperty("cluster","",false);
        String database = getProperty("database","e2e",true);
        String defaultTable = testPrefix + UUID.randomUUID().toString().replace('-', '_');
        String table = getProperty("table",defaultTable,true);

    }
}
