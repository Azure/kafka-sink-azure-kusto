package com.microsoft.azure.kusto.kafka.connect.sink.integeration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);
  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(45);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final int TASKS_MAX = 1;
  protected EmbeddedConnectCluster connect;
  protected void startConnect() throws Exception {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();
    connect.start();
  }

  protected void stopConnect() {
    try {
      connect.stop();
    } catch (Exception ne) {
      ne.printStackTrace();
    }
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the expected number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() == numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info.");
      return Optional.empty();
    }
  }

  protected Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector");
    props.put("bootstrap.servers",connect.kafka().bootstrapServers());
    props.put("topics","kafka1");
    props.put("tasks.max","1");
    props.put("kusto.url","xxxx");
    props.put("kusto.auth.authority","xxxx");
    props.put("kusto.auth.appid","xxxx");
    props.put("kusto.auth.appkey","xxxx");
    props.put("value.converter.schemas.enable","false");
    props.put("key.converter.schemas.enable","false");
    props.put("kusto.sink.flush_size", "10000");
    props.put("value.converter","org.apache.kafka.connect.storage.StringConverter");
    props.put("key.converter","org.apache.kafka.connect.storage.StringConverter");
    return props;
  }
}
