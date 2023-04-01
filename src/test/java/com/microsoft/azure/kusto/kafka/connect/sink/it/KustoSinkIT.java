package com.microsoft.azure.kusto.kafka.connect.sink.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import io.confluent.avro.random.generator.Generator;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.createConnectorJar;
import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.getConnectorProperties;

public class KustoSinkIT {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.5"))
            .withNetwork(network);
    private static final List<String> testFormats = List.of("avro"); // List.of("json", "avro", "csv", "raw"); // Raw for XML

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
            log.info("Creating tables in Kusto");
            createTables();
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
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                String.format("https://%s.kusto.windows.net/", coordinates.cluster),
                coordinates.appId, coordinates.appKey, coordinates.authority);
        try (Client engineClient = ClientFactory.createClient(engineCsb)) {
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
            try {
                produceKafkaMessages(dataFormat);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void produceKafkaMessages(String dataFormat) throws IOException {
        log.info("Producing messages");
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        // avro
        Generator.Builder builder = new Generator.Builder().schemaString(IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("it-avro.avsc")),
                StandardCharsets.UTF_8));
        Generator randomDataBuilder = builder.build();
        if (dataFormat.equals("avro")) {
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            // GenericRecords to bytes using avro
            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                for (int i = 0; i < 10; i++) {
                    GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
                    writer.write(record, encoder);
                    encoder.flush();
                    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>("e2e.avro.topic", "Key-" + i, outputStream.toByteArray());
                    producer.send(producerRecord);
                }
            }
        } else if (dataFormat.equals("json")) {
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
                    producer.send(producerRecord);
                }
            }

        }
        log.info("Produced messages");
    }
}
