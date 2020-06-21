package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonRecordWriterProviderTest {

  @Test
  public void testJsonData() throws IOException {
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    for(int i=0;i<10;i++) {
      Map< String,Integer> map = new HashMap< String,Integer>();
      map.put("hello",i);
      records.add(new SinkRecord("mytopic", 0, null, null, null, map, i));
    }
    File file = new File("abc.json");
    KustoSinkConfig config = new KustoSinkConfig(getProperties());
    JsonRecordWriterProvider jsonWriter = new JsonRecordWriterProvider();
    FileOutputStream fos = new FileOutputStream(file);
    OutputStream out=fos;
    RecordWriter rd = jsonWriter.getRecordWriter(config,file.getPath(),out);
    for(SinkRecord record : records){
      rd.write(record);
    }
    rd.commit();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String st;
    int i=0;
    while ((st = br.readLine()) != null) {
      assertEquals(st, String.format("{\"hello\":%s}", i));
      i++;
    }
    assertEquals(rd.getDataSize(),100);
    file.delete();
  }

  protected Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector");
    props.put("bootstrap.servers", "localhost:9092");
    props.put("topics", "kafka1");
    props.put("tasks.max", "1");
    props.put("kusto.url", "xxx");
    props.put("kusto.auth.authority", "xxx");
    props.put("kusto.auth.appid", "xxx");
    props.put("kusto.auth.appkey", "xxx");
    props.put("kusto.sink.tempdir", "/home/hasher/microsoft/kafka-sink-azure-kusto/src/test/resources/testE2E/csv");
    props.put("value.converter.schemas.enable", "false");
    props.put("key.converter.schemas.enable", "false");
    props.put("kusto.sink.flush_size", "10000");
    props.put("kusto.tables.topics.mapping", "[{'topic': 'xxx','db': 'xxx', 'table': 'xxx','format': 'avro', 'mapping':'avri'}]");
    return props;
  }
}

