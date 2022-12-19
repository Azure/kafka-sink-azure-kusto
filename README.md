# Azure Data Explorer Kafka Connect Kusto Sink Connector

This repository contains the source code of the Kafka Connect Kusto sink connector.<br>
**"Kusto"** is the Microsoft internal project code name for Azure Data Explorer, Microsoft Azure's big data analytical
database PaaS offering.

## Topics covered

[1. Overview](README.md#1-overview)<br>
[2. Integration design](README.md#2-integration-design)<br>
[3. Features supported](README.md#3-features-supported)<br>
[4. Connect worker properties](README.md#4-connect-worker-properties)<br>
[5. Sink properties](README.md#5-sink-properties)<br>
[6. Roadmap](README.md#6-roadmap)<br>
[7. Deployment overview](README.md#7-deployment-overview)<br>
[8. Connector download/build from source](README.md#8-connector-downloadbuild-from-source)<br>
[9. Test drive the connector - standalone mode](README.md#9-test-drive-the-connector---standalone-mode)<br>
[10. Distributed deployment details](README.md#10-distributed-deployment-details)<br>
[11. Test drive the connector - distributed mode](README.md#11-test-drive-the-connector---distributed-mode)<br>
[12. Apache Kafka version - Docker Kafka Connect base image version - Confluent Helm chart version related](README.md#12-apache-kafka-version---docker-kafka-connect-base-image-version---confluent-helm-chart-version-related)<br>
[13. Other](README.md#13-other)<br>
[14. Need Support?](README.md#14-need-support)<br>
[15. Major version specifics](README.md#15-major-version-specifics)<br>
[16. Release History](README.md#16-release-history)<br>
[17. Contributing](README.md#17-contributing)<br>


<hr>

## 1. Overview

Azure Data Explorer is a first party Microsoft big data analytical database PaaS, purpose built for low latency
analytics of all manners of logs, all manners of telemetry and time series data.<br>

Kafka ingestion to Azure Data Explorer leverages Kafka Connect. Kafka Connect is an open source Apache Kafka ETL service
for code-free, configuration based, scalable, fault tolerant integration with Kafka from/to any system through
development of connector plugins (integration code). The Azure Data Explorer team has developed a sink connector, that
sinks from Kafka to Azure Data Explorer. <br>

Our connector is **gold certified** by Confluent - has gone through comprehensive review and testing for quality,
feature completeness, compliance with standards and for performance.<br>

The connector is open source and we welcome community contribution.


<hr>

## 2. Integration design

Integration mode to Azure Data Explorer is queued or streaming ingestion leveraging the Azure Data Explorer Java SDK.
Events are dequeued from Kafka and per the flush* sink properties, batched or streamed, and shipped to Azure Data
Explorer as gzipped files. In case of batch ingestions, Azure Data Explorer has a configurable
table [ingest batching policy](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/batchingpolicy),
based on which ingestion into tables occurs automatically. In case of streaming ingestion, configure 'streaming' as '
true' in 'kusto.tables.topics.mapping', by default streaming is set as false.

<hr>

## 3. Features supported

### 3.1. Validation of required properties on start-up and fail-fast

- The connector fails fast if any of the "required" sink properties are unavailable
- The connector checks for access to the cluster and database table and shuts down if inaccessible
- The connector checks for any dependent properties that are selectively required and shuts down if unavailable (e.g. a
  converter specified that relies on schema registry but missing a schema registry URL)

### 3.2. Configurable behavior on errors

- The connector supports configurable
- shut down in the event of an error
  <br>OR
- log the error and process as many events as possible

### 3.3. Configurable retries

- The connector supports retries for transient errors with the ability to provide relevant parameters
- and retries with exponential backoff

### 3.4. Serialization formats

- The connector supports Avro, JSON, CSV formats
- It supports Parquet and ORC file formats with the ByteArrayConverter specified below

### 3.5. Schema registry

- The connector supports schema registry for avro and json

### 3.6. Schema evolution

- The connector does not support schema evolution currently

### 3.7. Kafka Connect converters

- The connector supports the following converters:

| #   | Converter | Details                                                  | 
|:----| :--- |:---------------------------------------------------------| 
| 1   | org.apache.kafka.connect.storage.StringConverter | Use with csv/json                                        |
| 2   | org.apache.kafka.connect.json.JsonConverter | Use with schemaless json                                 |
| 3   | io.confluent.connect.avro.AvroConverter | Use with avro                                            |
| 4   | io.confluent.connect.json.JsonSchemaConverter | Use with json with schema registry                       |
| 5   | org.apache.kafka.connect.converters.ByteArrayConverter | Use with ORC, Parquet files written as messages to Kafka |
| 6   | io.confluent.connect.protobuf.ProtobufConverter | Use with protobuf format with schema registry            |

### 3.8. Kafka Connect transformers

- The connector does not support transformers. Prefer transformation on the server side in Kafka or ingestion time in
  Azure Data Explorer
  with [update policies](https://docs.microsoft.com/azure/data-explorer/kusto/management/updatepolicy).

### 3.9. Topics to tables mapping

- The connector supports multiple topics to multiple tables configuration per Kafka Connect worker

### 3.10. Kafka Connect Dead Letter Queue

- The connector supports user provided "Dead Letter Queue", a Kafka Connect construct; E.g. If Avro messages are written
  to a "dead letter queue" topic that is expecting Json, the avro messages are written to a configurable dead letter
  queue instead of just being dropped. This helps prevent data loss and also data integrity checks for messages that did
  not make it to the destination. Note that for a secure cluster, in addition to bootstrap server list and topic name,
  the security mechanism, the security protocol, jaas config have to be provided for the Kafka Connect worker and in the
  sink properties

### 3.11. Miscellaneous Dead Letter Queue

- The connector supports user provided miscelleneous "Dead Letter Queue" for transient and non-deserialization errors (
  those not managed by Kafka Connect); E.g. If network connectivity is lost to Azure Data Explorer, the connector
  retries and eventually writes the queued up messages to the miscelleneous "Dead Letter Queue". Note that for a secure
  cluster, in addition to bootstrap server list and topic name, the security mechanism, the security protocol, jaas
  config have to be provided for the Kafka Connect worker and in the sink properties

### 3.12. Delivery semantics

- Azure Data Explorer is an append only immutable database. Infrastructure failures and unavoidable external variables
  that can lead to duplicates cant be remediated via upsert commands as upserts are not supported. <br>

Therefore the connector supports "At least once" delivery guarantees.

### 3.13. Overrides

- The connector supports overrides at the sink level *if overrides are specified at a Kafka Connect worker level*. This
  is a Kafka Connect feature, not specific to the Kusto connector plugin.

### 3.14. Parallelism

- As with all Kafka Connect connectors, parallelism comes through the setting of connector tasks count, a sink property

### 3.15. Authentication & Authorization to Azure Data Explorer

- Azure Data Explorer supports Azure Active Directory authentication. For the Kusto Kafka connector, we need an Azure
  Active Directory Service Principal created and "admin" permissions granted to the Azure Data Explorer database.
- The Service Principal can either be
  - an Enterprise Application, authenticated using the OAuth2 endpoint of Active Directory, using the Tenant ID,
    Application ID and Application Secret
  - a Managed Identity, using [the private Instance MetaData Service](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-java) accessible from within Azure VMs
    - for more information on managed identities, see [Managed Identities for Azure Resources](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/) and [AAD Pod Identity](https://github.com/Azure/aad-pod-identity) for AKS
    - in that scenario, the tenant ID and client ID of the managed identity can be deduced from the context of the call site and are optional

### 3.16. Security related

- Kafka Connect supports all security protocols supported by Kafka, as does our connector
- See below for some security related config that needs to be applied at Kafka Connect worker level as well as in the
  sink properties

<hr>

## 4. Connect worker properties

- There are some core configs that need to be set at the Kafka connect worker level. Some of these are security configs
  and the (consumer) override policy. These for e.g. need to be baked into
  the [Docker image](Readme.md#10-distributed-deployment-details) covered further on in this document-

### 4.1. Confluent Cloud

The below covers Confluent Cloud-<br>
[Link to end to end sample](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/5-configure-connector-cluster.md)

```
ENV CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY=All

ENV CONNECT_SASL_MECHANISM=PLAIN
ENV CONNECT_SECURITY_PROTOCOL=SASL_SSL
ENV CONNECT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https
ENV CONNECT_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<yourConfluentCloudAPIKey>\" password=\"<yourConfluentCloudAPISecret>\";"
```

### 4.2. HDInsight Kafka with Enterprise Security Package

The below covers HDInsight Kafka with Enterprise Security Package (Kerberos)-<br>
You will need the HDI privileged user keytab to consume from Kafka, the krb5.conf and jaas conf as well<br>
[Link to end to end sample](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/connectors-crud-esp.md)<br>
<br>

**1. Keytab:**<br>
Generate the keytab for the privileged user authorized to consume from Kafka in your HDI cluster as follows, lets call
the keytab, kafka-client-hdi.keytab-<br>

```
ktutil
addent -password -p <UPN>@<REALM> -k 1 -e RC4-HMAC
wkt kafka-client-hdi.keytab
```

scp this to your local machine to copy onto the Kafka Connect worker Docker image.

<br>

**2. krb5.conf:**<br>

The following is sample krb5.conf content; Create this on your local machine, modify to reflect your krb5 details; we
will use this to copy onto the Kafka Connect worker Docker image-<br>

```
[libdefaults]
        default_realm = <yourKerberosRealm>


[realms]
    <yourKerberosRealmName> = {
                admin_server = <yourKerberosRealmNameInLowerCase>
                kdc = <yourKerberosRealmNameInLowerCase>
                default_domain = <yourDomainInLowerCase>
        }

[domain_realm]
    <yourKerberosRealmNameInLowerCase> = <yourKerberosRealm>
    .<yourKerberosRealmNameInLowerCase> = <yourKerberosRealm>


[login]
        krb4_convert = true
        krb4_get_tickets = false
```

E.g.
Review
this [sample](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/connectors-crud-esp.md#54-lets-create-a-new-krb5conf)
.
<br>

**3. jaas.conf:**<br>

The following is sample jaas.conf content-<br>

```
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/kafka-client-hdi.keytab"
    principal="<yourUserPrincipal>@<yourKerberosRealm>";
};
```

<br>

**4. Configs to add to the Docker image:**<br>
This is covered in detail further on. It is specified here for the purpose of completeness of defining what goes onto
the worker config.<br>

```
COPY krb5.conf /etc/krb5.conf
COPY hdi-esp-jaas.conf /etc/hdi-esp-jaas.conf 
COPY kafka-client-hdi.keytab /etc/security/keytabs/kafka-client-hdi.keytab

ENV KAFKA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"

ENV CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY=All

ENV CONNECT_SASL_MECHANISM=GSSAPI
ENV CONNECT_SASL_KERBEROS_SERVICE_NAME=kafka
ENV CONNECT_SECURITY_PROTOCOL=SASL_PLAINTEXT
ENV CONNECT_SASL_JAAS_CONFIG="com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/etc/security/keytabs/kafka-client-hdi.keytab\" principal=\"<yourKerberosUPN>@<yourKerberosRealm>\";"
```

<hr>

## 5. Sink properties

The following is complete set of connector sink properties-

| #   | Property                                     | Purpose                                                                                         | Details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | 
|:----|:---------------------------------------------|:------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | connector.class                              | Classname of the Kusto sink                                                                     | Hard code to ```com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector```<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 2   | topics                                       | Kafka topic specification                                                                       | List of topics separated by commas<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 3   | kusto.ingestion.url                          | Kusto ingestion endpoint URL                                                                    | Provide the ingest URL of your ADX cluster<br>Use the following construct for the private URL - https://ingest-private-[cluster].kusto.windows.net<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 4   | kusto.query.url                              | Kusto query endpoint URL                                                                        | Provide the engine URL of your ADX cluster<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 5   | aad.auth.strategy                            | Credentials for Kusto                                                                           | Strategy to authenticate against Azure Active Directory, either ``application`` (default) or ``managed_identity``.<br>*Optional, `application` by default*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 6   | aad.auth.authority                           | Credentials for Kusto                                                                           | Provide the tenant ID of your Azure Active Directory<br>*Required when authentication is done with an `application` or when `kusto.validation.table.enable` is set to `true`*                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 7   | aad.auth.appid                               | Credentials for Kusto                                                                           | Provide Azure Active Directory Service Principal Name<br>*Required when authentication is done with an `application` or when `kusto.validation.table.enable` is set to `true`*                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 8   | aad.auth.appkey                              | Credentials for Kusto                                                                           | Provide Azure Active Directory Service Principal secret<br>*Required when authentication is done with an `application`*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 9   | kusto.tables.topics.mapping                  | Mapping of topics to tables                                                                     | Provide 1..many topic-table comma-separated mappings as follows-<br>[{'topic': '\<topicName1\>','db': '\<datebaseName\>', 'table': '\<tableName1\>','format': '<format-e.g.avro/csv/json>', 'mapping':'\<tableMappingName1\>','streaming':'false'},{'topic': '\<topicName2\>','db': '\<datebaseName\>', 'table': '\<tableName2\>','format': '<format-e.g.avro/csv/json>', 'mapping':'\<tableMappingName2\>','streaming':'false'}]<br>*Required* <br> Note : The attribute mapping (Ex:'mapping':''tableMappingName1') is an optional attribute. During ingestion, Azure Data Explorer automatically maps column according to the ingestion format |
| 10  | key.converter                                | Deserialization                                                                                 | One of the below supported-<br>org.apache.kafka.connect.storage.StringConverter<br> org.apache.kafka.connect.json.JsonConverter<br>io.confluent.connect.avro.AvroConverter<br>io.confluent.connect.json.JsonSchemaConverter<br> org.apache.kafka.connect.converters.ByteArrayConverter<br><br>*Required*                                                                                                                                                                                                                                                                                                                                          |
| 11  | value.converter                              | Deserialization                                                                                 | One of the below supported-<br>org.apache.kafka.connect.storage.StringConverter<br> org.apache.kafka.connect.json.JsonConverter<br>io.confluent.connect.avro.AvroConverter<br>io.confluent.connect.json.JsonSchemaConverter<br> org.apache.kafka.connect.converters.ByteArrayConverter<br><br>*Required*                                                                                                                                                                                                                                                                                                                                          |
| 12  | value.converter.schema.registry.url          | Schema validation                                                                               | URI of the Kafka schema registry<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 13  | value.converter.schemas.enable               | Schema validation                                                                               | Set to true if you have embedded schema with payload but are not leveraging the schema registry<br>Applicable for avro and json<br><br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 14  | tasks.max                                    | connector parallelism                                                                           | Specify the number of connector copy/sink tasks<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 15  | flush.size.bytes                             | Performance knob for batching                                                                   | Maximum bufer byte size per topic+partition combination that in combination with flush.interval.ms (whichever is reached first) should result in sinking to Kusto<br>*Default - 1 MB*<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 16  | flush.interval.ms                            | Performance knob for batching                                                                   | Minimum time interval per topic+partition combo that in combination with flush.size.bytes (whichever is reached first) should result in sinking to Kusto<br>*Default - 30 seconds*<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 17  | tempdir.path                                 | Local directory path on Kafka Connect worker to buffer files to before shipping to Kusto        | Default is value returned by ```System.getProperty("java.io.tmpdir")``` with a GUID attached to it<br><br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 18  | behavior.on.error                            | Configurable behavior in response to errors encountered                                         | Possible values - log, ignore, fail<br><br>log - log the error, send record to dead letter queue, and continue processing<br>ignore - log the error, send record to dead letter queue, proceed with processing despite errors encountered<br>fail - shut down connector task upon encountering<br><br>*Default - fail*<br>*Optional*                                                                                                                                                                                                                                                                                                              |
| 19  | errors.retry.max.time.ms                     | Configurable retries for transient errors                                                       | Period of time in milliseconds to retry for transient errors<br><br>*Default - 300 ms*<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 20  | errors.retry.backoff.time.ms                 | Configurable retries for transient errors                                                       | Period of time in milliseconds to backoff before retry for transient errors<br><br>*Default - 10 ms*<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 21  | errors.deadletterqueue.bootstrap.servers     | Channel to write records that failed deserialization                                            | CSV or kafkaBroker:port <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 22  | errors.deadletterqueue.topic.name            | Channel to write records that failed deserialization                                            | Pre-created topic name <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 23  | errors.deadletterqueue.security.protocol     | Channel to write records that failed deserialization                                            | Securitry protocol of secure Kafka cluster <br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 24  | errors.deadletterqueue.sasl.mechanism        | Channel to write records that failed deserialization                                            | SASL mechanism of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 25  | errors.deadletterqueue.sasl.jaas.config      | Channel to write records that failed deserialization                                            | JAAS config of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 26  | misc.deadletterqueue.bootstrap.servers       | Channel to write records that due to reasons other than deserialization                         | CSV of kafkaBroker:port <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 27  | misc.deadletterqueue.topic.name              | Channel to write records that due to reasons other than deserialization                         | Pre-created topic name <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 28  | misc.deadletterqueue.security.protocol       | Channel to write records that due to reasons other than deserialization                         | Securitry protocol of secure Kafka cluster <br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 29  | misc.deadletterqueue.sasl.mechanism          | Channel to write records that due to reasons other than deserialization                         | SASL mechanism of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 30  | misc.deadletterqueue.sasl.jaas.config        | Channel to write records that due to reasons other than deserialization                         | JAAS config of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 31  | consumer.override.bootstrap.servers          | Security details explicitly required for secure Kafka clusters                                  | Bootstrap server:port CSV of secure Kafka cluster <br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 32  | consumer.override.security.protocol          | Security details explicitly required for secure Kafka clusters                                  | Security protocol of secure Kafka cluster <br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 33  | consumer.override.sasl.mechanism             | Security details explicitly required for secure Kafka clusters                                  | SASL mechanism of secure Kafka cluster<br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 34  | consumer.override.sasl.jaas.config           | Security details explicitly required for secure Kafka clusters                                  | JAAS config of secure Kafka cluster<br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 35  | consumer.override.sasl.kerberos.service.name | Security details explicitly required for secure Kafka clusters, specifically kerberized Kafka   | Kerberos service name of kerberized Kafka cluster<br>*Required for kerberized Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 36  | consumer.override.auto.offset.reset          | Configurable consuming from offset                                                              | Possible values are - earliest or latest<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 37  | consumer.override.max.poll.interval.ms       | Config to prevent duplication                                                                   | Set to a value to avoid consumer leaving the group while the Connector is retrying <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 38  | kusto.validation.table.enable                | Validation config to verify the target table exists & the role of user has ingestion privileges | If true , validates existence of table & the princpal has ingestor role. Defaults to true , has to be explicitly set to false to disable this check (future release will make this opt-in as opposed to opt-out) <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 38  | proxy.host                                   | Host details of proxy server                                                                    | Host details of proxy server configuration <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 38  | proxy.port                                   | Port details of proxy server                                                                    | Port details of proxy server configuration <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |

<hr>

## 6. Streaming ingestion

Kusto supports [Streaming ingestion](https://docs.microsoft.com/azure/data-explorer/ingest-data-streaming) in order to
achieve sub-second latency.

This connector supports this
using [Managed streaming client](https://github.com/Azure/azure-kusto-java/blob/master/ingest/src/main/java/com/microsoft/azure/kusto/ingest/ManagedStreamingIngestClient.java)
.

Usage: configure per topic-table that streaming should be used. For example:

```
kusto.tables.topics.mapping=[{'topic': 't1','db': 'db', 'table': 't1','format': 'json', 'mapping':'map', 'streaming': true}].
```

Requirements: Streaming enabled on the
cluster. [Streaming policy](https://docs.microsoft.com/azure/data-explorer/kusto/management/streamingingestionpolicy)
configured on the table or database.

Additional configurations: flush.size.bytes and flush.interval.ms are still used to batch
records together before ingestion - flush.size.bytes should not be over 4MB, flush.interval.ms
is suggested to be low (hundreds of milliseconds).
We still recommend configuring ingestion batching policy at the table or database level, as the client falls back to
queued ingestion in case of failure and retry-exhaustion.

## 7. Roadmap

The following is the roadmap-<br>

| # | Roadmap item| 
| :--- | :--- |
| 1 | Schema evolution support |

<hr>

## 8. Deployment overview

Kafka Connect connectors can be deployed in standalone mode (just for development) or in distributed mode (production)
.<br>

### 8.1. Standalone Kafka Connect deployment mode

This involves having the connector plugin jar in /usr/share/java of a Kafka Connect worker, reference to the same plugin
path in connect-standalone.properties, and launching of the connector from command line. This is not scalable, not fault
tolerant, and is not recommeded for production.

### 8.2. Distributed Kafka Connect deployment mode

Distributed Kafka Connect essentially involves creation of a KafkaConnect worker cluster as shown in the diagram
below.<br>

- Azure Kubernetes Service is a great infrastructure for the connect cluster, due to its managed and scalable nature
- Kubernetes is a great platform for the connect cluster, due to its scalable nature and self-healing
- Each orange polygon is a Kafka Connect worker and each green polygon is a sink connector instance
- A Kafka Connect worker can have 1..many task instances which helps with scale
- When a Kafka Connect worker is maxed out from a resource perspective (CPU, RAM), you can scale horizontally, add more
  Kafka Connect workers, ands tasks within them
- Kafka Connect service manages rebalancing of tasks to Kafka topic partitions automatically without pausing the
  connector tasks in recent versions of Kafka
- A Docker image needs to be created to deploy the Kusto sink connector in a distributed mode. This is detailed below.

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-ADX.png)
<br>
<br>
<hr>
<br>

## 9. Connector download/build from source

Multiple options are available-

### 9.1. Download a ready-to-use uber jar from our Github repo releases listing

https://github.com/Azure/kafka-sink-azure-kusto/releases

### 9.2. Download the connector from Confluent Connect Hub

https://www.confluent.io/hub/microsoftcorporation/kafka-sink-azure-kusto/

### 9.3. Build uber jar from source

The dependencies are-

* JDK >= 1.8 [download](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
* Maven [download](https://maven.apache.org/install.html)

**1. Clone the repo**<br>

```bash
git clone git://github.com/Azure/kafka-sink-azure-kusto.git
cd ./kafka-sink-azure-kusto
```

**2. Build with maven**<br>
For an Uber jar, run the below-

```bash
mvn clean compile assembly:single
```

For the connector jar along with jars of associated dependencies, run the below-

```bash
mvn clean install
```

Look
within `target/components/packages/microsoftcorporation-kafka-sink-azure-kusto-<version>/microsoftcorporation-kafka-sink-azure-kusto-<version>/lib/`
folder



<hr>

## 10. Test drive the connector - standalone mode

In a standalone mode (not recommended for production), the connector can be test driven in any of the following ways-

### 10.1. Self-contained Dockerized setup

[Review this hands on lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/dockerized-quickstart/README.md)
. It includes dockerized kafka, connector and Kafka producer to take away complexities and allow you to focus on the
connector aspect.

### 10.2. HDInsight Kafka, on an edge node

[Review this hands on lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/standalone-mode/README.md)
. The lab referenced may have slightly outdated list of sink properties. Modify them to make current, leveraging the
latest sink properties detailed in [section 5](README.md#5-sink-properties).

<hr>

## 11. Distributed deployment details

The following are the components and configuration needed in place.<br>

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/connector-CRUD.png)
<br>
<br>
<hr>
<br>

The following section strives to explain further, pictorially, what's involved with distributed deployment of the
connector-<br>

### 11.1. Docker image creation

1. Create a Docker Hub account if it does not exist
2. Install Docker desktop on your machine
3. Build a docker image for the KafkaConnect worker that include any connect worker level configurations, and the Kusto
   connector jar
4. Push the image to the Docker hub
   <br>

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-Image-Creation.png)
<br>
<br>
<hr>
<br>

### 11.2. Provision Kafka Connect workers on an Azure Kubernetes Service cluster

5. Provision KafkaConnect workers on our Azure Kubernetes Service cluster

When we start off, all we have is an empty Kubernetes cluster-

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-Empty.png)
<br>
<br>
<hr>
<br>

When we are done, we have a live KafkaConnect cluster that is integrated with Confluent Cloud-

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-KafkaConnect.png)
<br>
<br>
<hr>
<br>

Note: This still does not have copy tasks (connector tasks) running yet

### 11.3. Postman for Kafka Connect REST APIs or REST calls from your CLI

6. Install Postman on our local machine<br>
7. Import KafkaConnect REST call JSON collection from Github into Postman<br>
   https://github.com/Azure/azure-kusto-labs/blob/confluent-clound-hol/kafka-integration/confluent-cloud/rest-calls/Confluent-Cloud-ADX-HoL-1-STUB.postman_collection.json<br>

OR<br>

8. Find the REST calls [here](https://docs.confluent.io/current/connect/references/restapi.html) to call from CLI

Note: Depending on Kafka security
configuration, [update the security configuration in the sink properties](https://docs.confluent.io/current/connect/security.html#authentication)
.

### 11.4. Launch the connector tasks using the Kafka Connect REST API

9. Launch the Kafka-ADX copy tasks/REST call, otherwise called connector tasks from Postman or via curl command

This is what we will see, a Kusto sink connector cluster with copy tasks running.

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-Connector-Cluster.png)
<br>
<br>
<hr>
<br>

Note: The diagram below depicts just one connector task per Kafka Connect worker. You can actually run 1..many connector
tasks till you max out the capacity.

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-ADX.png)
<br>
<br>
<hr>
<br>

Section 11, below, links to a hands-on lab to test drive the deployment. The lab is end to end. Prefer
the [Confluent cloud lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/confluent-kafka/README.md)
for the simplest deployment.



<hr>

## 12. Test drive the connector - distributed mode

The labs referenced below may have slightly outdated list of sink properties. Modify them to make current, leveraging
the latest sink properties detailed
in [section 5](https://github.com/microsoft/kusto-kafka-feature/blob/master/docs/ConnectorGitDoc.md#5-sink-properties).

### 12.1. HDInsight Kafka

For a non-secured HDInsight Kafka cluster
- [run through this end-to-end hands-on-lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/README.md)
<br>
For a secured HDInsight Kafka cluster (Kerberised)
-  [review these details specific to HDInsight with Enterprise Security Package](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/connectors-crud-esp.md)

### 12.2. Confluent Cloud

[Run through this end-to-end hands-on-lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/README.md)

### 12.3. Confluent IaaS (operator based)

[Run through this end-to-end hands-on-lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/confluent-kafka/README.md)


<hr>

## 13. Apache Kafka version - Docker Kafka Connect base image version - Confluent Helm chart version related

### 13.1. Docker image

We have deliberately not published a Docker image due to the multitude of versions of Apache Kafka bundled into the
Confluent platform versions, and multiple versions of our Kafka connector, not to mention the security configs specific
to each customer's Kafka deployment. We therefore recommend that a custom image be developed using
the [appropriate version](https://hub.docker.com/r/confluentinc/cp-kafka-connect/tags) compatible with the Apache Kafka
version.

### 13.2. Helm chart

Similarly, we recommend leveraging the right version of the Helm
chart.  [Confluent base helm chart for Kafka Connect](https://github.com/confluentinc/cp-helm-charts).


<hr>

## 14. Other

### 14.1. Feedback, issues and contribution

The connector plugin is open source. We welcome feedback, and contribution. Log an issue, in the issues tab as needed.
See section 14.

### 14.2. Scaling out/in

- Connector tasks can be scaled out per Kafka Connect worker by pausing, editing and resuming the connector cluster
- When tasks per worker are maxed out, Azure Kubernetes Service cluster can be scaled out for more nodes, and Kafka
  Connect workers can be provisioned on the same

### 14.3. Sizing

- Confluent recommends Kafka Connect workers with minimum of 4 cores and 16 GB of RAM
- Start with 3 workers (3 AKS nodes), and scale horizontally as needed
- Number of tasks should ideally be equal to the number of Kafka topic partitions, not more
- Play with the number of tasks, wokers, nodes till you see the performance you desire

### 14.4. Performance tuning

- Kafka topic: number of partitions should be tuned for performance
- Connectors: AKS right-sizing, connector tasks right-sizing, configure the right values for flush.size.bytes and
  flush.interval.ms
- Kusto: Right-size Kusto cluster for ingestion (SKU and node count), tune the table or database
  [ingestion batching policy](https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy)
- Format: Avro (with schema registry) and CSV perform more-or-less similarly from tests done

### 14.5. Upgrading to version 1.x from prior versions

To upgrade, you would have to stop the connector tasks, recreate your connect worker Docker image to include the latest
jar, update the sink properties to leverage the renamed and latest sink properties, reprovision the connect workers,
then launch the copy tasks. You can use the consumer.override* feature to manipulate offset to read from.
<hr>

## 15. Need Support?

- **Found a bug?** Please help us fix it by thoroughly documenting it
  and [filing an issue](https://github.com/Azure/kafka-sink-azure-kusto/issues/new).
- **Have a feature request?** Please post it
  on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask
  on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access
  to [support](https://docs.microsoft.com/azure/azure-supportability/how-to-create-azure-support-request) with
  guaranteed response time. Consider submitting a ticket and get assistance from Microsoft support team

## 16. Major version specifics

With version 1.0, we overhauled the connector. The following are the changes-

1. We renamed some properties for consistency with standards
2. Added support for schema registry
3. Added support for more converters - we supported only stringConverter and ByteArrayConverter previously
4. Improved upfront validation and fail fast
5. Added support for configurable behavior on error
6. Added support for configurable retries
7. Added support for Kafka Connect dead letter queues
8. Introduced additional dead letter queue property for those errors that are not handled by Kafka Connect through its
   dead letter queue feature
9. Improved the delivery guarantees to "at least once" (no data loss)

Here is
our [blog post](https://techcommunity.microsoft.com/t5/azure-data-explorer/azure-data-explorer-kafka-connector-new-features-with-version-1/ba-p/1637143)
.

To upgrade, you would have to stop the connector tasks, recreate your connect worker Docker image to include the latest
jar, update the sink properties to leverage the renamed and latest sink properties, reprovision the connect workers,
then launch the copy tasks.
<hr>

For information about what changes are included in each release, please see
the [Release History](README.md#16-release-history) section of this document.

## 17. Release History

| Release Version | Release Date | Changes Included                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|-----------------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0.1.0           | 2020-03-05   | <ul><li>Initial release</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 1.0.1           | 2020-08-04   | <ul><li>New feature: flush interval - stop aggregation by timer</li><li>New feature: Support orc avro and parquet via 1 file per message. kusto java sdk version</li><li>Bug fix: Connector didn't work well with the New java version</li><li>Bug fix: Fixed usage of compressed files and binary types</li><li>Bug fix: Client was closed when kafka task was called close() certain partitions. Now closing only on stop. Issue resulted in no refresh of the ingestion resources and caused failure on ingest when trying to add message to the azure queue.</li><li>Bug fix: In certain kafka pipelines - the connector files were deleted before ingestion.</li><li>New feature: Support for dlq</li><li>New feature: Support json and avro schema registry</li><li>New feature: Support json and avro converters</li><li>Bug fix: Correct committed offset value to be (+ 1) so as not to ingest last record twice</li></ul> |
| 1.0.2           | 2020-10-06   | <ul><li>Bug fix: Cast of count of records to long instead of int, to accommodate larger databases.</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 1.0.3           | 2020-10-13   | <ul><li>Bug fix: Fix Multijson usage</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 2.0.0           | 2020-11-12   | <ul><li>Bug fix: Trying to create a new directory failed probably because it was already created due to a race condition.</li><li>Bug fix: Resetting the timer was not behind lock, which could result in a race condition of it being destroyed by other code.</li><li>New feature: Added required kusto.query.url parameter so that we can now specify a Kusto Query URL that isn't simply the default of the Kusto Ingestion URL prepended with "ingest-".</li><li>New feature: Renamed the kusto.url parameter to kusto.ingestion.url for clarity and consistency.</li></ul>                                                                                                                                                                                                                                                                                                                                                    |
| 2.1.0           | 2021-07-11   | <ul><li>Upgrade Kusto Java SDK to 2.8.2.</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 2.2.0           | 2021-09-13   | <ul><li>New feature: Streaming ingestion has been added</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 3.0.0           | 2022-06-06   | <ul><li>New feature: Internal default batch set to 30 seconds</li><li>New feature: Update kusto sdk to latest version 3.1.1</li><li>Bug fix: Flush timer close / fix NPE</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 3.0.1           | 2022-06-13   | <ul><li>Bug fix:Close could ingest a file after offsets commit - causing duplications</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |             |
| 3.0.2           | 2022-07-20   | <ul><li>New feature: Changes to support protobuf data ingestion</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 3.0.3           | 2022-08-09   | <ul><li>Bug fix: Library upgrade to fix CVE-2020-36518 Out-of-bounds Write</li></ul>
| 3.0.4           | 2022-09-05   | <ul><li>New feature: Make mapping optional , fixes Issue#76</li><li>New feature: Make table validation optional when the connector starts up (Refer: kusto.validation.table.enable)</li><li>Bug fix: Stop collecting messages when DLQ is not enabled. Provides better scaling & reduces GC pressure</li></ul>
| 3.0.5           | 2022-09-07   | <ul><li>New feature: Support authentication with Managed Identities</li></ul>
| 3.0.6           | 2022-11-28   | <ul><li>Upgrade Kusto Java SDK to 3.2.1 and fix failing unit test case (mitigate text4shell RCE vulnerability)</li></ul>
| 3.0.7           | 2022-12-06   | <ul><li>Upgrade Jackson version to the latest security version</li><li>Filter tombstone records & records that fail JSON serialization</li></ul>
| 3.0.8           | 2022-12-15   | <ul><li>New feature: Added Proxy support to KustoSinkTask</li></ul>
| 3.0.9           | 2022-12-19   | <ul><li>Bugfix: Restrict file permissions on file created for ingestion</li><li>Canonicalize file names</li><li>Refactor tests</li></ul>
## 17. Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

In order to make the PR process efficient, please follow the below checklist:

* **There is an issue open concerning the code added** - Either a bug or enhancement. Preferably the issue includes an
  agreed upon approach.
* **PR comment explains the changes done** - This should be a TL;DR; as the rest of it should be documented in the
  related issue.
* **PR is concise** - Try to avoid making drastic changes in a single PR. Split it into multiple changes if possible. If
  you feel a major change is needed, make sure the commit history is clear and maintainers can comfortably review both
  the code and the logic behind the change.
* **Please provide any related information needed to understand the change** - Especially in the form of unit tests, but
  also docs, guidelines, use-case, best practices, etc as appropriate.
* **Checks should pass**

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
