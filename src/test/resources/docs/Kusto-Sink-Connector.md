# Azure Kusto Sink Connector
The Azure Kusto Sink Connector is used to read records form Kafka topics and ingest them into Kusto Tables.

## Features       


The Azure Kusto Sink Connector offers the following features:     

-  **At-least-Once Semantics**: The connector creates a new emtry inot the Kusto table for each record in Kafka topic. However, duplicates are still possible to occur when failure, rescheduling or re-configuration happens. This semantics is followed when `behavior.on.error` is set to `fail` mode. In case of `log` and `ignore` modes, the connector promises at-most semantics.
-  **Automatic Retries**: The Azure Kusto Sink Connector may experience network failures while connecting to the Kusto Ingestion URI. The connector will automatically retry with an exponential backoff to ingest records. The property `errors.retry.max.time.ms`  controls the maximum time until which the connector will retry ingesting the records.

---
## Prerequisites

The following are required to run the Azure Kusti SInk Connector:

- Kafka Broker: Confluent Platform 3.3.0 or above, or Kafka 0.11.0 or above
- Connect: Confluent Platform 4.0.0 or above, or Kafka 1.0.0 or above
- Java 1.8
- Kusto Ingestion URL

----
## Installation
#### Prerequisite
[Confluent Platform](https://www.confluent.io/download) must be installed.

#### Clone
Clone the [Azure Kusto Sink Connector](https://github.com/Azure/kafka-sink-azure-kusto)
```shell script
git clone git://github.com/Azure/kafka-sink-azure-kusto.git
cd ./kafka-sink-azure-kusto
```
#### Build
##### Requirements

- Java >=1.8
- Maven

Build the connector locally using Maven to produce complete Jar with dependencies
```
mvn clean compile assembly:single
```
:grey_exclamation: Move the jar inside a folder in Share/java folder within the Confluent Platform installation directory.


----
## Configuration Properties
For a complete list of configuration properties for this connector, see [Azure Kusto Sink Connector Configuration Properties](<// Todo add Link>).

---
## Quick Start
The quick start guide uses the Azure Sink Connector to consume records from a Kafka topic and ingest records into Kusto tables.

### Kusto Table and Table Mapping Setup

Before using the Azure Kusto Sink Connector, we are required to setup the table and its corresponding table mapping depending on our record schema, and our converter type.

 Use the following to [create a sample table](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/create-table-command) in Kusto.
 
 ```
.create table SampleKustoTable ( Name: string, Age:int)
```

Since, we would be using the Avro Converter for the demo, we will create an Avro-based table [ingestion mapping](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/create-ingestion-mapping-command) using the following code Snippet.
```
.create table SampleKustoTable ingestion avro mapping "SampleAvroMapping"
'['
'    { "column" : "Name", "datatype" : "string", "Properties":{"Path":"$.Name"}},'
'    { "column" : "Age", "datatype" : "int", "Properties":{"Path":"$.Age"}}'
']'
```
> **Note**   
> Properties are the corresponding Field in you record schema.

For more information about ingestion mapping, see [Kusto Data mappings](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/mappings).

### Start Confluent
Start the Confluent services using the following [Confluent CLI](https://docs.confluent.io/current/cli/index.html#cli) command:

 ```shell script
confluent local start
```
> **Important**    
> Do not use the Confluent CLI in production environments. 

### Property-based example
Create a configuration file `kusto-sink.properties` with the following content. this file should
be placed inside the Confluent Platform installation directory. This configuration is used typically along with standalone workers.

```
name=azure-0
connector.class=com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector
topics=SampleAvroTopic
tasks.max=1

kusto.tables.topics.mapping=[{'topic': 'SampleAvroTopic','db': 'DatabaseName', 'table': 'SampleKustoTable','format': 'avro', 'mapping':'SampleAvroMapping'}]

kusto.url=https://ingest-<your cluster URI>.kusto.windows.net
aad.auth.authority=****
aad.auth.appid=****
aad.auth.appkey=****
tempdir.path=/var/temp/

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081

behavior.on.error=LOG
dlq.bootstrap.servers=localhost:9092
dlq.topic.name=dlq-topic-log
errors.retry.max.time.ms=2000
errors.retry.backoff.time.ms=1000
```

Run the connector with this configuration.
```shell script
confluent local load kusto-sink-connector -- -d kusto-sink.properties
```

The output should resemble:
```json
{
  "name": "kusto-sink-connector",
  "config": {
    "connector.class": "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector",
    "topics": "SampleAvroTopic",
    "tasks.max": "1",
    "kusto.tables.topics.mapping": "[{'topic': 'SampleAvroTopic','db': 'DatabaseName', 'table': 'SampleKustoTable','format': 'avro', 'mapping':'SampleAvroMapping'}]",
    "kusto.url": "https://ingest-<your cluster URI>.kusto.windows.net",
    "aad.auth.authority": "****",
    "aad.auth.appid": "****",
    "aad.auth.appkey": "****",
    "tempdir.path": "/var/temp/",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "behavior.on.error": "LOG",
    "dlq.bootstrap.servers": "localhost:9092",
    "dlq.topic.name": "dlq-topic-log",
    "name": "kusto-sink-connector"
  },
  "tasks": [],
  "type": "sink"
}
```

Confirm that the connector is in a RUNNING state.
```shell script
confluent local status kusto-sink-connector
```

The output should resemble:

```shell script
{
  "name": "kusto-sink-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "127.0.1.1:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "127.0.1.1:8083"
    }
  ],
  "type": "sink"
}
```

### REST-based example
Use this setting with [distributed workers](https://docs.confluent.io/current/connect/concepts.html#distributed-workers). 
Write the following JSON to `config.json`, configure all the required values, and use the following command to post the configuration to one of the distributed connect workers. 
Check here for more information about the Kafka Connect [REST API](https://docs.confluent.io/current/connect/references/restapi.html#connect-userguide-rest)

```json
{
    "name": "KustoSinkConnectorCrimes",
    "config": {

	"connector.class":"com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector",
	"topics":"SampleAvroTopic",
	"tasks.max":1,
	"kusto.tables.topics.mapping":[{"topic": "SampleAvroTopic","db": "DatabaseName", "table": "SampleKustoTable","format": "avro", "mapping":"SampleAvroMapping"}],
	"kusto.url":"https://ingest-azureconnector.centralus.kusto.windows.net",
	"aad.auth.authority":"****",
	"aad.auth.appid":"****",
	"aad.auth.appkey":"****",
	"tempdir.path":"/var/tmp",
	"key.converter":"org.apache.kafka.connect.storage.StringConverter",
	"value.converter":"io.confluent.connect.avro.AvroConverter",
	"value.converter.schema.registry.url":"http://localhost:8081",
	"behavior.on.error":"LOG",
	"dlq.bootstrap.servers":"localhost:9092",
	"dlq.topic.name":"dlq-topic-log"
    }
}
```
>**Note**    
>Change the `confluent.topic.bootstrap.servers` property to include your broker address(es) and change the `confluent.topic.replication.factor` to 3 for staging or production use.

Use curl to post a configuration to one of the Kafka Connect workers. Change `http://localhost:8083/` to the endpoint of one of your Kafka Connect worker(s).
```shell script
curl -sS -X POST -H 'Content-Type: application/json' --data @config.json http://localhost:8083/connectors
```
Use the following command to update the configuration of existing connector.
```shell script
curl -s -X PUT -H 'Content-Type: application/json' --data @config.json http://localhost:8083/connectors/kusto-sink-connector/config
```
Confirm that the connector is in a `RUNNING` state by running the following command:
```
curl http://localhost:8083/connectors/pagerduty-sink-connector/status | jq
```

The output should resemble:

```shell script
{
  "name": "kusto-sink-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "127.0.1.1:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "127.0.1.1:8083"
    }
  ],
  "type": "sink"
}
```

To produce Avro data to Kafka topic: `SampleAvroTopic`, use the following command.
```shell script
./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic SampleAvroTopic --property value.schema='{"type":"record","name":"details","fields":[{"name":"Name","type":"string"}, {"name":"Age","type":"int"}]}'
```

While the console is waiting for the input, use the following three records and paste each of them on the console.
```shell script
{"Name":"Alpha Beta", "Age":1}
{"Name":"Beta Charlie", "Age":2}
{"Name":"Charlie Delta", "Age":3}
```

Finally, check the Kusto table `SampleKustoTable` to see the newly ingested records.

---
## Additional Documentation

- [Azure Kusto Sink Connector Configuration Property]()
- [Changelog]()      






