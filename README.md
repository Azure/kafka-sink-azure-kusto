# Microsoft Azure Data Explorer (Kusto) Kafka Sink 
This repository contains the source code of the Kafka To ADX Sink.


## Setup

### AKS

Deploying Kafka -> ADX connector on AKS

Setting up ADX connector on AKS is a useful setup for a scalable solution of connecting Kafka and ADX.
The setup is rather easy:

Pre-requirements:
* Running Kafka ([confluent](https://docs.confluent.io/current/installation/operator/co-deployment.html))
* Running Kusto ([docs](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal))
* Kubectl installed ([docs](https://kubernetes.io/docs/tasks/tools/install-kubectl/))

Setup is comprised of the following steps:

1. Setup an AKS cluster
2. Deploy Kafka with ADX connector
3. Post configuration for ADX connector
4. Test
5. Check for errors
6. Monitor Performance


#### Setup AKS cluster
Straightforward. Go to Azure Portal, add a Kubernetes Service, and go through the wizard.

In order to be able to work with the cluster using `kubectl`, you can sync it with azure cli credentials.

Make sure you are logged in properly:
```
az account show
```

If it is the wrong subscription, switch to the correct one:
```
az account set --subscription <subscription_id>
```

Then, to get the credentials for aks, run:

```
az aks get-credentials -g <group_name> -n <kafka_name>
```

Now, let see that kubectl can properly access the cluster:
```
kubectl get nodes
```

*UI*:  If you are familiar with Kubernetes Dashboard, you can use it as well - [docs](https://docs.microsoft.com/en-us/azure/aks/kubernetes-dashboard)

#### Deploy Kafka-ADX Connector

##### Docker Image

Since we need the Kusto Connector to be bundled with confluent's kafka cluster, we need an image.

```docker
FROM confluentinc/cp-kafka-connect:5.2.1 
COPY ./binary/kafka-sink-azure-kusto-0.1.0-jar-with-dependencies.jar /usr/share/java
```

This image already [exists](https://hub.docker.com/r/jojokoshy/kafka-connect-kusto), and so we will use it.

##### Using Confluent helm chart



##### Deploy & Manage (REST API)

Following commands are used to control the connector:

DEPLOY connector
```json
{
    "url":  "http://localhost:803/connectors/",
    "method": "POST",
    "header": [
        {
            "key": "Accept",
            "value": "application/json",
            "type": "text"
        },
        {
            "key": "Content-Type",
            "value": "application/json",
            "type": "text"
        }
    ],
    "body": {        
        "name": "KustoSinkConnector",
        "config": {
            "connector.class": "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector",
            "kusto.sink.flush_interval_ms": "10000",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
            "tasks.max": "20",
            "topics": "test",
            "kusto.tables.topics_mapping": "[{
                'topic': 'test',
                'db': 'testdb', 
                'table': 'testtable',
                'format': 'json', 
                'mapping':'mapping'
            }]",            
            "kusto.url":"https://ingest-cluster.region.kusto.windows.net",
            "kusto.auth.authority": "<tenant_id>",
            "kusto.auth.appid":"<app_id>",
            "kusto.auth.appkey":"<app_key>",
            "kusto.sink.tempdir":"/var/tmp/",
            "kusto.sink.flush_size":"100000"
        }        
    },
    
}
```

GET connector
```json
{
    "url": "http://localhost:803/connectors/",
    "method": "GET", 
}
```

UPDATE config
```json
{
    "url":  "http://localhost:803/connectors/KustoSinkConnector/config"
    "method": "PUT",       
    "header": [
        {
            "key": "Accept",
            "type": "text",
            "value": "application/json"
        },
        {
            "key": "Content-Type",
            "type": "text",
            "value": "application/json"
        }
    ],
    "body": {
        "name": "KustoSinkConnector",
        "config": {
            "connector.class": "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector",
            "kusto.sink.flush_interval_ms": "10000",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
            "tasks.max": "20",
            "topics": "test",
            "kusto.tables.topics_mapping": "[{
                'topic': 'test',
                'db': 'testdb', 
                'table': 'testtable',
                'format': 'json', 
                'mapping':'mapping'
            }]",                
            "kusto.url":"https://ingest-cluster.region.kusto.windows.net",
            "kusto.auth.authority": "<tenant_id>",
            "kusto.auth.appid":"<app_id>",
            "kusto.auth.appkey":"<app_key>",
            "kusto.sink.tempdir":"/var/tmp/",
            "kusto.sink.flush_size":"100000"
        }
    }
}
```

GET Connector status
```json
{
    "url":  "http://localhost:803/connectors/KustoSinkConnector/status",
    "method": "GET",			
}
```

RESTART Connector
```json
{
    "url":  "http://localhost:803/connectors/KustoSinkConnector/restart",
    "method": "POST"
}
```

GET Connector config
```json
{   
    "url":  "http://localhost:803/connectors/KustoSinkConnector/config",     
    "method": "GET"    
}
```

DELETE Connector
```json
{
    "url":  "http://localhost:803/connectors/KustoSinkConnector/",
    "method": "DELETE"
}
```

#### Test

#### Check for errors

#### Monitor traffic

### Standalone

#### Clone

```bash
git clone git://github.com/Azure/kafka-sink-azure-kusto.git
cd ./kafka-sink-azure-kusto
```

#### Build

Need to build locally with Maven 

##### Requirements

* JDK >= 1.8 [download](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
* Maven [download](https://maven.apache.org/install.html)

Building locally using Maven is simple:

```bash
mvn clean compile assembly:single
```

Which should produce a Jar complete with dependencies.

#### Deploy 

Deployment as a Kafka plugin will be demonstrated using a docker image for convenience,
but production deployment should be very similar (detailed docs can be found [here](https://docs.confluent.io/current/connect/userguide.html#installing-plugins))

#### Run Docker
```bash
docker run --rm -p 3030:3030 -p 9092:9092 -p 8081:8081 -p 8083:8083 -p 8082:8082 -p 2181:2181  -v C:\kafka-sink-azure-kusto\target\kafka-sink-azure-kusto-0.1.0-jar-with-dependencies.jar:/connectors/kafka-sink-azure-kusto-0.1.0-jar-with-dependencies.jar landoop/fast-data-dev 
```

#### Verify 
connect to container and run:
`cat /var/log/broker.log /var/log/connect-distributed.log | grep -C 4 i kusto`

#### Add plugin 
Go to `http://localhost:3030/kafka-connect-ui/#/cluster/fast-data-dev/` and using the UI add Kusto Sink (NEW button, then pick kusto from list)
example configuration:

```config
name=KustoSinkConnector 
connector.class=com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector 
kusto.sink.flush_interval_ms=300000 
key.converter=org.apache.kafka.connect.storage.StringConverter 
value.converter=org.apache.kafka.connect.storage.StringConverter 
tasks.max=1 
topics=testing1 
kusto.tables.topics_mapping=[{'topic': 'testing1','db': 'daniel', 'table': 'KafkaTest','format': 'json', 'mapping':'Mapping'}] 
kusto.auth.authority=XXX 
kusto.url=https://ingest-mycluster.kusto.windows.net/ 
kusto.auth.appid=XXX 
kusto.auth.appkey=XXX 
kusto.sink.tempdir=/var/tmp/ 
kusto.sink.flush_size=1000
```

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
Make sure no errors happend duting ingestion
```
.show ingestion failures
```
See that newly ingested data becomes available for querying
```
KafkaTest | count
```

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