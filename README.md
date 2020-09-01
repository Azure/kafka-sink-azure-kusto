# Azure Data Explorer Kafka Connect Kusto Sink Connector
This repository contains the source code of the Kafka Connect Kusto sink connector.<br>
**"Kusto"** is the Microsoft internal project code name for Azure Data Explorer, Microsoft Azure's big data analytical database PaaS offering.

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
[12. Docker image specifics](README.md#12-docker-image-specifics)<br>
[13. Version specifics](README.md#13-version-specifics)<br>
[14. Other](README.md#14-other)<br>


<hr>

## 1. Overview
Azure Data Explorer is a first party Microsoft big data analytical database PaaS, purpose built for low latency analytics of all manners of logs, all manners of telemetry and time series data.<br>

Kafka ingestion to Azure Data Explorer leverages Kafka Connect.  Kafka Connect is an open source Apache Kafka ETL service for code-free, configuration based, scalable, fault tolerant integration with Kafka from/to any system through development of connector plugins (integration code). The Azure Data Explorer team has developed a sink connector, that sinks from Kafka to Azure Data Explorer. <br>

Our connector is **gold certified** by Confluent - has gone through comprehensive review and testing for quality, feature completeness, compliance with standards and for performance.<br>

The connector is open source and we welcome community contribution.


<hr>


## 2. Integration design

Integration mode to Azure Data Explorer is batched, queued ingestion leveraging the Azure Data Explorer Java SDK.  Events are dequeued from Kafka and per the flush* sink properties, batched, and shipped to Azure Data Explorer as gzipped files, and queued for ingestion.  Azure Data Explorer has a configurable table [ingest batching policy](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/batchingpolicy), based on which ingestion into tables occurs automatically.

<hr>

## 3. Features supported

### 3.1. Validation of required properties on start-up and fail-fast
- The connector fails fast if any of the "required" sink properties are unavailable
- The connector checks for access to the cluster and database table and shuts down if inaccessible
- The connector checks for any dependent properties that are selectively required and shuts down if unavailable (e.g. a converter specified that relies on schema registry but missing a schema registry URL)

### 3.2. Configurable behavior on errors 
- The connector supports configurable
- shut down in the event of an error
<br>OR
- log the error and process as many events as possible


### 3.3. Configurable retries
- The connector supports retries for transient errors with the ability to provide parameters for the same
- and retries with exponential backoff

### 3.4.  Serialization formats
- The connector supports Avro, JSON, CSV formats
- It supports Parquet and ORC file formats with the ByteArrayConverter specified below

### 3.5.  Schema registry
- The connector supports schema registry for avro and json

### 3.6.  Schema evolution
- The connector does not support schema evolution currently

### 3.7.  Kafka Connect converters
- The connector supports the following converters:

| # | Converter | Details | 
| :--- | :--- | :--- | 
| 1 | org.apache.kafka.connect.storage.StringConverter | Use with csv/json |
| 2 | org.apache.kafka.connect.json.JsonConverter | Use with schemaless json |
| 3 | io.confluent.connect.avro.AvroConverter | Use with avro |
| 4 | io.confluent.connect.json.JsonSchemaConverter | Use with json with schema registry |
| 5 | org.apache.kafka.connect.converters.ByteArrayConverter | Use with ORC, Parquet files written as messages to Kafka |

### 3.8.  Kafka Connect transformers
- The connector does not support transformers. Prefer transformation on the server side in Kafka or ingestion time in Azure Data Explorer with [update policies](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/updatepolicy).


### 3.9. Topics to tables mapping
- The connector supports multiple topics to multiple tables configuration per Kafka Connect worker

### 3.10. Kafka Connect Dead Letter Queue
- The connector supports user provided "Dead Letter Queue", a Kafka Connect construct; E.g. If Avro messages are written to a "dead letter queue" topic that is expecting Json, the avro messages are written to a configurable dead letter queue instead of just being dropped.  This helps prevent data loss and also data integrity checks for messages that did not make it to the destination.  Note that for a secure cluster, in addition to bootstrap server list and topic name, the security mechanism, the security protocol, jaas config have to be provided for the Kafka Connect worker and in the sink properties

### 3.11. Miscellaneous Dead Letter Queue
- The connector supports user provided miscelleneous "Dead Letter Queue" for transient and non-deserialization errors (those not managed by Kafka Connect); E.g. If network connectivity is lost to Azure Data Explorer, the connector retries and eventually writes the queued up messages to the miscelleneous "Dead Letter Queue".  Note that for a secure cluster, in addition to bootstrap server list and topic name, the security mechanism, the security protocol, jaas config have to be provided for the Kafka Connect worker and in the sink properties

### 3.12. Delivery semantics
- Azure Data Explorer is an append only immutable database.  Infrastructure failures and unavoidable external variables that can lead to duplicates cant be remediated via upsert commands as upserts are not supported. <br>

Therefore the connector supports "At least once" delivery guarantees.

### 3.13. Overrides
- The connector supports overrides at the sink level *if overrides are specified at a Kafka Connect worker level*.  This is a Kafka Connect feature, not specific to the Kusto connector plugin.

### 3.14. Parallelism
- As with all Kafka Connect connectors, parallelism comes through the setting of connector tasks count, a sink property

### 3.15. Authentication & Authroization to Azure Data Explorer
- Azure Data Explorer supports Azure Active Directory authentication.  For the Kusto Kafka connector, we need an Azure Active Directory Service Principal created and "admin" permissions granted to the Azure Data Explorer database. 

### 3.16. Security related
- Kafka Connect supports all security protocols supported by Kafka, as does our connector
- See below for some security related config that needs to be applied at Kafka Connect worker level as well as in the sink properties


<hr>

## 4. Connect worker properties
- There are some core configs that need to be set at the Kafka connect worker level.  Some of these are security configs and the (consumer) override policy. These for e.g. need to be baked into the [Docker image](ConnectorReadme.md#10-distributed-deployment-details) covered further on in this document-

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
Generate the keytab for the privileged user authorized to consume from Kafka in your HDI cluster as follows, lets call the keytab, kafka-client-hdi.keytab-<br>
```
ktutil
addent -password -p <UPN>@<REALM> -k 1 -e RC4-HMAC
wkt kafka-client-hdi.keytab
```
scp this to your local machine to copy onto the Kafka Connect worker Docker image.

<br>

**2. krb5.conf:**<br>

The following is sample krb5.conf content; Create this on your local machine, modify to reflect your krb5 details; we will use this to copy onto the Kafka Connect worker Docker image-<br>
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
Review this [sample](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/connectors-crud-esp.md#54-lets-create-a-new-krb5conf).
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
This is covered in detail further on.  It is specified here for the purpose of completenes of defining what goes onto the worker config.<br>
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

| # | Property | Purpose | Details | 
| :--- | :--- | :--- | :--- | 
| 1 | connector.class | Classname of the Kusto sink | Hard code to ``` com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector ```<br>*Required* |
| 2 | topics | Kafka topic specification | List of topics separated by commas<br>*Required*  |
| 3 | kusto.url | Kusto ingest cluster specification | Provide the ingest URI of your ADX cluster<br>*Required*  |
| 4 | aad.auth.authority | Credentials for Kusto | Provide the tenant ID of your Azure Active Directory<br>*Required*  |
| 5 | aad.auth.appid | Credentials for Kusto  | Provide Azure Active Directory Service Principal Name<br>*Required*  |
| 6 | aad.auth.appkey | Credentials for Kusto  | Provide Azure Active Directory Service Principal secret<br>*Required*  |
| 7 | kusto.tables.topics.mapping | Mapping of topics to tables  | Provide 1..many topic-table comma-separated mappings as follows-<br>[{'topic': '\<topicName1\>','db': '\<datebaseName\>', 'table': '\<tableName\>','format': '<format-e.g.avro/csv/json>', 'mapping':'\<tableMappingName\>'}]<br>*Required*  |
| 8 | key.converter | Deserialization | One of the below supported-<br>org.apache.kafka.connect.storage.StringConverter<br> org.apache.kafka.connect.json.JsonConverter<br>io.confluent.connect.avro.AvroConverter<br>io.confluent.connect.json.JsonSchemaConverter<br> org.apache.kafka.connect.converters.ByteArrayConverter<br><br>*Required*  |
| 9 | value.converter | Deserialization | One of the below supported-<br>org.apache.kafka.connect.storage.StringConverter<br> org.apache.kafka.connect.json.JsonConverter<br>io.confluent.connect.avro.AvroConverter<br>io.confluent.connect.json.JsonSchemaConverter<br> org.apache.kafka.connect.converters.ByteArrayConverter<br><br>*Required*  |
| 10 | value.converter.schema.registry.url | Schema validation | URI of the Kafka schema registry<br>*Optional*  |
| 11 | value.converter.schemas.enable | Schema validation | Set to true if you have embedded schema with payload but are not leveraging the schema registry<br>Applicable for avro and json<br><br>*Optional*  |
| 12 | tasks.max | connector parallelism | Specify the number of connector copy/sink tasks<br>*Required*  |
| 13 | flush.size.bytes | Performance knob for batching | Maximum bufer byte size per topic+partition combination that in combination with flush.interval.ms (whichever is reached first) should result in sinking to Kusto<br>*Default - 1 MB*<br>*Required*  |
| 14 | flush.interval.ms | Performance knob for batching | Minimum time interval per topic+partition combo that in combination with flush.size.bytes (whichever is reached first) should result in sinking to Kusto<br>*Default - 300 ms*<br>*Required*  |
| 15 | tempdir.path | Local directory path on Kafka Connect worker to buffer files to before shipping to Kusto | Default is value returned by ```System.getProperty("java.io.tmpdir")``` with a GUID attached to it<br><br>*Optional*  |
| 16 | behavior.on.error | Configurable behavior in response to errors encountered | Possible values - log, ignore, fail<br><br>log - log the error, send record to dead letter queue, and continue processing<br>ignore - log the error, send record to dead letter queue, proceed with processing despite errors encountered<br>fail - shut down connector task upon encountering<br><br>*Default - fail*<br>*Optional*  |
| 17 | errors.retry.max.time.ms | Configurable retries for transient errors | Period of time in milliseconds to retry for transient errors<br><br>*Default - 300 ms*<br>*Optional*  |
| 18 | errors.retry.backoff.time.ms | Configurable retries for transient errors | Period of time in milliseconds to backoff before retry for transient errors<br><br>*Default - 10 ms*<br>*Optional*  |
| 19 | errors.deadletterqueue.bootstrap.servers | Channel to write records that failed deserialization | CSV or kafkaBroker:port <br>*Optional*  |
| 20 | errors.deadletterqueue.topic.name | Channel to write records that failed deserialization | Pre-created topic name <br>*Optional*  |
| 21 | errors.deadletterqueue.security.protocol | Channel to write records that failed deserialization  | Securitry protocol of secure Kafka cluster <br>*Optional but when feature is used with secure cluster, is required*  |
| 22 | errors.deadletterqueue.sasl.mechanism | Channel to write records that failed deserialization | SASL mechanism of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*  |
| 23 | errors.deadletterqueue.sasl.jaas.config | Channel to write records that failed deserialization | JAAS config of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*  |
| 24 | misc.deadletterqueue.bootstrap.servers | Channel to write records that due to reasons other than deserialization | CSV of kafkaBroker:port <br>*Optional*  |
| 25 | misc.deadletterqueue.topic.name | Channel to write records that due to reasons other than deserialization | Pre-created topic name <br>*Optional*  |
| 26 | misc.deadletterqueue.security.protocol | Channel to write records that due to reasons other than deserialization  | Securitry protocol of secure Kafka cluster <br>*Optional but when feature is used with secure cluster, is required*  |
| 27 | misc.deadletterqueue.sasl.mechanism | Channel to write records that due to reasons other than deserialization | SASL mechanism of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*  |
| 28 | misc.deadletterqueue.sasl.jaas.config | Channel to write records that due to reasons other than deserialization | JAAS config of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*  |
| 29 | consumer.override.bootstrap.servers | Security details explicitly required for secure Kafka clusters  | Bootstrap server:port CSV of secure Kafka cluster <br>*Required for secure Kafka clusters*  |
| 30 | consumer.override.security.protocol | Security details explicitly required for secure Kafka clusters  | Security protocol of secure Kafka cluster <br>*Required for secure Kafka clusters*  |
| 31 | consumer.override.sasl.mechanism | Security details explicitly required for secure Kafka clusters | SASL mechanism of secure Kafka cluster<br>*Required for secure Kafka clusters*  |
| 32 | consumer.override.sasl.jaas.config | Security details explicitly required for secure Kafka clusters | JAAS config of secure Kafka cluster<br>*Required for secure Kafka clusters*  |
| 33 | consumer.override.sasl.kerberos.service.name | Security details explicitly required for secure Kafka clusters, specifically kerberized Kafka | Kerberos service name of kerberized Kafka cluster<br>*Required for kerberized Kafka clusters*  |
| 34 | consumer.override.auto.offset.reset | Configurable consuming from offset | Possible values are - earliest or latest<br>*Optional*  |
| 35 | consumer.override.max.poll.interval.ms| Config to prevent duplication | Set to a value to avoid consumer leaving the group while the Connector is retrying <br>*Optional*  |



<hr>


## 6. Roadmap
The following is the roadmap-<br>

| # | Roadmap item| 
| :--- | :--- |
| 1 | Streaming ingestion support |
| 2 | Schema evolution support |
| 3 | Protobuf support*  - converter, schema registry<br>*This item is based on demand AND availability of native Protobuf ingestion support*|



<hr>

## 7. Deployment overview
Kafka Connect connectors can be deployed in standalone mode (just for development) or in distributed mode (production).<br>

### 7.1. Standalone Kafka Connect deployment mode 
This involves having the connector plugin jar in /usr/share/java of a Kafka Connect worker, reference to the same plugin path in connnect-standalone.properties, and launching of the connector from command line.  This is not scalable, not fault tolerant, and is not recommeded for production.

### 7.2. Distributed Kafka Connect deployment mode 
Distributed Kafka Connect essentially involves creation of a KafkaConnect worker cluster as shown in the diagram below.<br>
- Azure Kubernetes Service is a great infrastructure for the connect cluster, due to its managed and scalabale nature
- Kubernetes is a great platform for the connect cluster, due to its scalabale nature and self-healing
- Each orange polygon is a Kafka Connect worker and each green polygon is a sink connector instance
- A Kafka Connect worker can have 1..many task instances which helps with scale
- When a Kafka Connect worker is maxed out from a resource perspective (CPU, RAM), you can scale horizontally, add more Kafka Connect workers, ands tasks within them
- Kafka Connect service manages rebalancing of tasks to Kafka topic partitions automatically without pausing the connector tasks in recent versions of Kafka
- A Docker image needs to be created to deploy the Kusto sink connector in a distributed mode.  This is detailed below.

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-ADX.png)
<br>
<br>
<hr>
<br>


## 8. Connector download/build from source

Multiple options are available-

### 8.1. Download a ready-to-use uber jar from our Github repo releases listing
https://github.com/Azure/kafka-sink-azure-kusto/releases

### 8.2. Download the connector from Confluent Connect Hub
<to be added>

### 8.3. Build uber jar from source
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

Look within `target/components/packages/microsoftcorporation-kafka-sink-azure-kusto-<version>/microsoftcorporation-kafka-sink-azure-kusto-<version>/lib/` folder



<hr>


## 9. Test drive the connector - standalone mode
In a standalone mode (not recommended for production), the connector can be test driven in any of the following ways-

### 9.1. Self-contained Dockerized setup

[Review this hands on lab]().  It includes dockerized kafka, connector and Kafka producer to take away complexities and allow you to focus on the connector aspect.



### 9.2. HDInsight Kafka, on an edge node
[Review this hands on lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/standalone-mode/README.md).  The lab referenced may have slightly outdated list of sink properties.  Modify them to make current, leveraging the latest sink properties detailed in [section 5](README.md#5-sink-properties).

<hr>


## 10. Distributed deployment details
The following are the components and configuration needed in place.<br>

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/connector-CRUD.png)
<br>
<br>
<hr>
<br>

The following section strives to explain further, pictorially, what's involved with distributed deployment of the connector-<br>

### 10.1. Docker image creation

1.  Create a Docker Hub account if it does not exist
2.  Install Docker desktop on your machine
3.  Build a docker image for the KafkaConnect worker that include any connect worker level configurations, and the Kusto connector jar
4.  Push the image to the Docker hub
<br>

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-Image-Creation.png)
<br>
<br>
<hr>
<br>

### 10.2. Provision Kafka Connect workers on an Azure Kubernetes Service cluster

5.  Provision KafkaConnect workers on our Azure Kubernetes Service cluster

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


### 10.3. Postman for Kafka Connect REST APIs or REST calls from your CLI

6.  Install Postman on our local machine<br>
7.  Import KafkaConnect REST call JSON collection from Github into Postman<br>
https://github.com/Azure/azure-kusto-labs/blob/confluent-clound-hol/kafka-integration/confluent-cloud/rest-calls/Confluent-Cloud-ADX-HoL-1-STUB.postman_collection.json<br>

OR<br>
8.  Find the REST calls [here](https://docs.confluent.io/current/connect/references/restapi.html) to call from CLI

Note: Depending on Kafka security configuration, [update the security configuration in the sink properties](https://docs.confluent.io/current/connect/security.html#authentication).

### 10.4. Launch the connector tasks using the Kafka Connect REST API

9.  Launch the Kafka-ADX copy tasks/REST call, otherwise called connector tasks from Postman or via curl command

This is what we will see, a Kusto sink connector cluster with copy tasks running.

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-Connector-Cluster.png)
<br>
<br>
<hr>
<br>

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-ADX.png)
<br>
<br>
<hr>
<br>

Section 11, below, links to a hands-on lab to test drive the deployment.  The lab is end to end.  Prefer the [Confluent cloud lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/confluent-kafka/README.md) for the simplest deployment.



<hr>

## 11. Test drive the connector - distributed mode

The labs referenced below may have slightly outdated list of sink properties.  Modify them to make current, leveraging the latest sink properties detailed in [section 5](https://github.com/microsoft/kusto-kafka-feature/blob/master/docs/ConnectorGitDoc.md#5-sink-properties).

### 11.1. HDInsight Kafka
For a non-secured HDInsight Kafka cluster - [run through this end-to-end hands-on-lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/README.md) 
<br>
For a secured HDInsight Kafka cluster (Kerberised) -  [review these details specific to HDInsight with Enterprise Security Package](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/hdinsight-kafka/connectors-crud-esp.md)

### 11.2. Confluent Cloud
[Run through this end-to-end hands-on-lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/confluent-kafka/README.md)

### 11.3. Confluent IaaS (operator based)
[Run through this end-to-end hands-on-lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/distributed-mode/confluent-kafka/README.md)


<hr>


## 12. Apache Kafka version - Docker Kafka Connect base image version - Confluent Helm chart version related

### 12.1. Docker image
We have deliberately not published a Docker image due to the multitude of versions of Apache Kafka bundled into the Confluent platform versions, and multiple versions of our Kafka connector, not to mention the security configs specific to each customer's Kafka deployment.  We therefore recommend that a custom image be developed using the [appropriate version](https://hub.docker.com/r/confluentinc/cp-kafka-connect/tags) compatible with the Apache Kafka version.

### 12.2. Helm chart
Similarly, we recommend leveraging the right version of the Helm chart.  [Confluent base helm chart for Kafka Connect](https://github.com/confluentinc/cp-helm-charts).


<hr>

## 13. Major version specifics
With the connector version 1.0.0, we overhauled the connector; We renamed some properties for consistency with standards, and removed some sink properties to stay consistent with how Kafka is used, and added multiple new properties, and also improved the delivery semantics from "at most once" to "at least once".    


<hr>

## 14. Other


### 14.1. Feedback, issues and contribution
The connector plugin is open source.  We welcome feedback, and contribution.  Log an issue, in the issues tab as needed.  See section 15.

### 14.2. Scaling out/in
- Connector tasks can be scaled out per Kafka Connect worker by pausing, editing and resuming the connector cluster
- When tasks per worker are maxed out, Azure Kubernetes Service cluster can be scaled out for more nodes, and Kafka Connect workers can be provisioned on the same


### 14.3. Sizing
- Confluent recommends Kafka Connect workers with minimum of 4 cores and 16 GB of RAM
- Start with 3 workers (3 AKS nodes), and scale horizontally as needed
- Number of tasks should ideally be equal to the number of Kafka topic partitions, not more
- Play with the number of tasks, wokers, nodes till you see the performance you desire

### 14.4. Performance tuning
- Kafka topic: number of partitions should be tuned for performance
- Connectors: AKS right-sizing, connector tasks right-sizing, configure the right values for flush.size.bytes and flush.interval.ms
- Kusto: Right-size Kusto cluster for ingestion (SKU and node count), tune the batch ingestion policy
- Format: Avro (with schema registry) and CSV perform more-or-less similarly from tests done


## 15. Need Support?
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Azure/kafka-sink-azure-kusto/issues/new).
- **Have a feature request?** Please post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://docs.microsoft.com/en-us/azure/azure-supportability/how-to-create-azure-support-request) with guaranteed response time.  Consider submitting a ticket and get assistance from Microsoft support team

## 16 Contribute

We gladly accept community contributions.

- Source Code Contributions: If you would like to become an active contributor to this project please follow the instructions provided in [Contributing.md](CONTRIBUTING.md).

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

