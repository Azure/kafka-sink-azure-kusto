# Microsoft Azure Data Explorer (Kusto) Kafka Sink 
This repository contains the source code of the Kafka To ADX Sink.


## Setup

### Clone

```bash
git clone git://github.com/Azure/kafka-sink-azure-kusto.git
cd ./kafka-sink-azure-kusto
```

### Build

Need to build locally with Maven 

#### Requirements

* JDK >= 1.8 [download](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
* Maven [download](https://maven.apache.org/install.html)

Building locally using Maven is simple:

```bash
mvn clean compile assembly:single
```

Which should produce a Jar complete with dependencies.

### Deploy 

Deployment as a Kafka plugin will be demonstrated using a docker image for convenience,
but production deployment should be very similar (detailed docs can be found [here](https://docs.confluent.io/current/connect/userguide.html#installing-plugins))

#### Run Docker
```bash
docker run --rm -p 3030:3030 -p 9092:9092 -p 8081:8081 -p 8083:8083 -p 8082:8082 -p 2181:2181  -v C:\kafka-sink-azure-kusto\target\kafka-sink-azure-kusto-0.1.0-jar-with-dependencies.jar:/connectors/kafka-sink-azure-kusto-0.1.0-jar-with-dependencies.jar landoop/fast-data-dev 
```

#### Verify 
Connect to container and run:

```bash
cat /var/log/broker.log /var/log/connect-distributed.log | grep -C 4 i kusto
```

#### Add plugin 
Go to `http://localhost:3030/kafka-connect-ui/#/cluster/fast-data-dev/` and using the UI add Kusto Sink (NEW button, then pick kusto from list)
example configuration:

```config

name=KustoSinkConnector 
connector.class=com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector 

key.converter=org.apache.kafka.connect.storage.StringConverter 
value.converter=org.apache.kafka.connect.storage.StringConverter 

tasks.max=1 
topics=testing1,testing2

kusto.tables.topics.mapping=[{'topic': 'testing1','db': 'test_db', 'table': 'test_table_1','format': 'json', 'mapping':'JsonMapping'},{'topic': 'testing2','db': 'test_db', 'table': 'test_table_2','format': 'csv', 'mapping':'CsvMapping'}] 

kusto.url=https://ingest-mycluster.kusto.windows.net/ 

aad.auth.appid
aad.auth.appkey
aad.auth.authority

kusto.sink.tempdir=/var/tmp/ 
flush.size.bytes=1000
flush.interval.ms=300000

behavior.on.error=FAIL

dlq.bootstrap.servers=localhost:9092
dlq.topic.name=test-topic-error

errors.retry.max.time.ms=60000
errors.retry.backoff.time.ms=5000
````

Aggregation in the sink is done using files, these are sent to kusto if the aggregated file has reached the flush_size 
(size is in bytes) or if the flush_interval_ms interval has passed. 
For the confluent parameters please refer here https://docs.confluent.io/2.0.0/connect/userguide.html#configuring-connectors
For scaling you should consider making tasks.max equal to the number of pods and ports.

#### Create Table and Mapping
Very similar to (Event Hub)[https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-event-hub#create-a-target-table-in-azure-data-explorer]

#### Publish data
In container, you can run interactive cli producer like so:
```bash
/usr/local/bin/kafka-console-producer --broker-list localhost:9092 --topic testing1
```

or just pipe file (which contains example data)
```bash
/usr/local/bin/kafka-console-producer --broker-list localhost:9092 --topic testing1 < file.json
```

#### Query Data
Make sure no errors happened during ingestion
```
.show ingestion failures
```
See that newly ingested data becomes available for querying
```
KafkaTest | count
```


#### Supported formats
`csv`, `json`, `avro`, `apacheAvro`, `tsv`, `scsv`, `sohsv`, `psv`, `txt`.

> Note - `avro` and `apacheAvro`files are sent each record (file) separately without aggregation, and are expected to be sent as a byte array containing the full file.
> 
>Use `value.converter=org.apache.kafka.connect.converters.ByteArrayConverter`



#### Avro example
One can use this gist [FilesKafkaProducer]("https://gist.github.com/ohadbitt/8475dc9f63df1c0d0bc322e9b00fdd00") to create
a JAR file that can be used as a file producer which sends files as bytes to kafka. 
* Create an avro file as in `src\test\resources\data.avro`
* Copy the jar `docker cp C:\Users\ohbitton\IdeaProjects\kafka-producer-test\target\kafka-producer-all.jar <container id>:/FilesKafkaProducer.jar`
* Connect to the container `docker exec -it <id> bash`.
* Run from the container `java -jar FilesKafkaProducer.jar fileName [topic] [times]`

## Need Support?
- **Have a feature request for SDKs?** Please post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://docs.microsoft.com/en-us/azure/azure-supportability/how-to-create-azure-support-request) with guaranteed response time.  Consider submitting a ticket and get assistance from Microsoft support team
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Azure/kafka-sink-azure-kusto/issues/new).

# Contribute

We gladly accept community contributions.

- Issues: Please report bugs using the Issues section of GitHub
- Forums: Interact with the development teams on StackOverflow or the Microsoft Azure Forums
- Source Code Contributions: If you would like to become an active contributor to this project please follow the instructions provided in [Contributing.md](CONTRIBUTING.md).

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

For general suggestions about Microsoft Azure please use our [UserVoice forum](http://feedback.azure.com/forums/34192--general-feedback).