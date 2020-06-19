package com.microsoft.azure.kusto.kafka.connect.sink.integeration;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;


public class AzureKustoConnectorIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(AzureKustoConnectorIT.class);

  private static final List<String> KAFKA_TOPICS = Arrays.asList("kafka1");
  private static final int NUM_OF_PARTITION = 5;
  private static final int NUM_RECORDS_PRODUCED_PER_PARTITION = 100;
  private static final String CONNECTOR_NAME = "azure-kusto-connector";
  private String appId = System.getProperty("appId","xxxx");
  private String appKey = System.getProperty("appKey","xxxxx");
  private String authority = System.getProperty("authority","xxxx");
  private String cluster = System.getProperty("cluster","xxxx");
  private String database = System.getProperty("database","xxxxx");
  Client engineClient;
  String table;

  @Before
  public void setup() throws Exception {
    startConnect();
    ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://%s.kusto.windows.net", cluster), appId, appKey, authority);
    engineClient = ClientFactory.createClient(engineCsb);
  }
  private String basePath = Paths.get("src/test/resources/", "testE2E").toString();
  @After
  public void close() {
    stopConnect();
  }

  @Test
  public void testWithCsvData() throws DataClientException, DataServiceException, IOException, InterruptedException {
    table = "CsvTable";
    Map<String, String> props = getProperties();
    props.put("tempdir.path",Paths.get(basePath, "csv").toString());
    props.put("value.converter","org.apache.kafka.connect.storage.StringConverter");
    props.put("key.converter","org.apache.kafka.connect.storage.StringConverter");
    connect.kafka().createTopic(KAFKA_TOPICS.get(0), NUM_OF_PARTITION);
    produceCsvRecords();
      engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
      engineClient.execute(database, String.format(".create table ['%s'] ingestion csv mapping 'mappy' " +
          "'[" +
          "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"transform\":\"SourceLocation\"}}," +
          "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Ordinal\":\"1\"}}," +
          "]'", table));
      // start a sink connector
     props.put("kusto.tables.topics.mapping", "[{'topic': 'kafka1','db':'" + database + "', 'table': '" + table + "','format': 'csv', 'mapping':'mappy'}]");
     connect.configureConnector(CONNECTOR_NAME, props);
      // wait for tasks to spin up
      waitForConnectorToStart(CONNECTOR_NAME, 1);
      log.error("Waiting for records in destination topic ...");
      validateExpectedResults(NUM_RECORDS_PRODUCED_PER_PARTITION * NUM_OF_PARTITION);
      engineClient.execute(database, ".drop table " + table);
  }

  @Test
  public void testWithJsonData() throws Exception {
    table = "JsonTable";
    connect.kafka().createTopic(KAFKA_TOPICS.get(0), NUM_OF_PARTITION);
    produceJsonRecords();
    engineClient.execute(database, String.format(".create table %s (TimeStamp: datetime, Name: string, Metric: int, Source:string)",table));
    engineClient.execute(database, String.format(".create table %s ingestion json mapping 'jsonMapping' '[{\"column\":\"TimeStamp\",\"path\":\"$.timeStamp\",\"datatype\":\"datetime\"},{\"column\":\"Name\",\"path\":\"$.name\",\"datatype\":\"string\"},{\"column\":\"Metric\",\"path\":\"$.metric\",\"datatype\":\"int\"},{\"column\":\"Source\",\"path\":\"$.source\",\"datatype\":\"string\"}]'",table));

    Map<String, String> props = getProperties();
    props.put("tempdir.path",Paths.get(basePath, "json").toString());
    props.put("kusto.tables.topics.mapping","[{'topic': 'kafka1','db': 'anmol', 'table': '"+ table +"','format': 'json', 'mapping':'jsonMapping'}]");
    props.put("value.converter","org.apache.kafka.connect.json.JsonConverter");
    props.put("key.converter","org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable","false");
    props.put("key.converter.schemas.enable","false");
    props.put("max.retries","0");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);
    log.error("Waiting for records in destination topic ...");
    validateExpectedResults(NUM_RECORDS_PRODUCED_PER_PARTITION * NUM_OF_PARTITION);
    engineClient.execute(database, ".drop table " + table);
  }

  private void produceCsvRecords(){
    for(int i = 0; i<NUM_OF_PARTITION;i++){
      for (int j = 0; j < NUM_RECORDS_PRODUCED_PER_PARTITION; j++) {
        String kafkaTopic = KAFKA_TOPICS.get(j % KAFKA_TOPICS.size());
        log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic);
        connect.kafka().produce(kafkaTopic,i, null,String.format("stringy message,%s",i));
      }
    }
  }

  private void produceJsonRecords(){
    for(int i = 0; i<NUM_OF_PARTITION;i++){
      for (int j = 0; j < NUM_RECORDS_PRODUCED_PER_PARTITION; j++) {
        String kafkaTopic = KAFKA_TOPICS.get(j % KAFKA_TOPICS.size());
        log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic);
        connect.kafka().produce(kafkaTopic,i, null,String.format("{\"timestamp\" : \"2017-07-23 13:10:11\",\"name\" : \"Anmol\",\"metric\" : \"%s\",\"source\" : \"Demo\"}",i));
      }
    }
  }

  private void validateExpectedResults(Integer expectedNumberOfRows) throws InterruptedException, DataClientException, DataServiceException {
    String query = String.format("%s | count", table);

    KustoResultSetTable res = engineClient.execute(database, query).getPrimaryResults();
    res.next();
    Integer timeoutMs = 60 * 9 * 1000;
    Integer rowCount = res.getInt(0);
    Integer timeElapsedMs = 0;
    Integer sleepPeriodMs = 5 * 1000;

    while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
      Thread.sleep(sleepPeriodMs);
      res = engineClient.execute(database, query).getPrimaryResults();
      res.next();
      rowCount = res.getInt(0);
      timeElapsedMs += sleepPeriodMs;
    }
    Assertions.assertEquals(rowCount, expectedNumberOfRows);
    this.log.info("Succesfully ingested " + expectedNumberOfRows + " records.");
  }
}
