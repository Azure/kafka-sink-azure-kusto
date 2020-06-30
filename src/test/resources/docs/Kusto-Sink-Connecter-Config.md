# Azure Kusto Sink Connector Configuration Properties
    
To use this connector, specify the name of the connector class in the `connector.class` configuration property.

```
connector.class=com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector
```

Connector-specific configuration properties are described below.

---
## Connection

##### `kusto.url`  

Kusto cluster url for ingestion.   
For Example: `https://ingest-clustername.kusto.windows.net`
- Type: string
- Importance: High


##### `kusto.auth.appid`   
Kusto AppID for authentication. 
- Type: string
- Importance: High
       

##### `kusto.auth.appkey`   
Kusto AppKey for authentication.
- Type: string
- Importance: High

##### `kusto.auth.authority`   
Kusto authority for authentication.
- Type: string
- Importance: High

## Write

##### `kusto.tables.topics.mapping`
Kusto target tables mapping(per topic mapping)    
For Example:    
```
[{'topic': 'testing1','db': 'test_db', 'table': 'test_table_1','format': 'json', 'mapping':'JsonMapping'},{'topic': 'testing2','db': 'test_db', 'table': 'test_table_2','format': 'csv', 'mapping':'CsvMapping', 'eventDataCompression':'gz'}] 
```
- Type: string
- Importance: High


##### `tempdir.path`
Temp dir that will be used by kusto sink to buffer records.
- Type: string
- Default: System temp Directory
- Importance: Low


##### `flush.size.bytes`
Kusto sink max buffer size (per topic+partition combo).
    medium
    1048576
- Type: int
- Default: 1048576
- Importance: medium

##### `flush.interval.ms`
Kusto sink max staleness in milliseconds (per topic+partition combo).
- Type: int
- Default: 300000
- Importance: medium


## Retries

##### `behavior.on.error`
Behavior on error setting for ingestion of records into Kusto table.  
Must be configured to one of the following:
        
`fail`   
Stops the connector when an error occurs while processing records or ingesting records in Kusto table.
        
`ignore`   
Continues to process next set of records when error occurs while processing records or ingesting records in Kusto table.
        
`log`   
Logs the error message and continues to process subsequent records when an error occurs while processing records or ingesting records in Kusto table, available in connect logs.

- Type: string
- Default: FAIL
- Valid values: [FAIL, IGNORE, LOG]
- Importance: Low
    
##### `dlq.bootstrap.servers`   
Configure this list to Kafka broker's address(es) to which the Connector should write failed records to. This list should be in the form host-1:port-1,host-2:port-2,…host-n:port-n. 
- Type: List
- Default: ""
- Importance: Low   


##### `dlq.topic.name`   
Set this to the Kafka topic's name to which the failed records are to be sinked.
- Type: string
- Default: ""
- Importance: Low 

 
##### `errors.retry.max.time.ms`   
Maximum time upto which the Connector should retry writing records to Kusto table in case of failures.
- Type: long
- Default: 300000
- Importance: Low    


##### `errors.retry.backoff.time.ms`
BackOff time between retry attempts the Connector makes to ingest records into Kusto table.
- Type: long
- Default: 10000
- Importance: Low  