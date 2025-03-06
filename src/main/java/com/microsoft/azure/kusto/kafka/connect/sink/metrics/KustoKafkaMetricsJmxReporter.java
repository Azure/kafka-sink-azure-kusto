package com.microsoft.azure.kusto.kafka.connect.sink.metrics;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import static com.microsoft.azure.kusto.kafka.connect.sink.metrics.KustoKafkaMetricsUtil.JMX_METRIC_PREFIX;

/**
 * Helper class for creation of JMX Metrics from metrics registry, also includes a definition to
 * create an ObjectName used to register a {@link com.codahale.metrics.Metric}
 *
 * <p>This instance is separate for all pipes and hence registration and unregistration of metrics
 * is handled per pipe level.
 */
public class KustoKafkaMetricsJmxReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KustoKafkaMetricsJmxReporter.class);

  // The registry which will hold pool of all metrics for this instance
  private final MetricRegistry metricRegistry;

  /**
   * Wrapper on top of listeners and metricRegistry for codehale. This will be useful to start the
   * jmx metrics when time is appropriate. (Check {@link KustoKafkaMetricsJmxReporter#start()})
   */
  private final JmxReporter jmxReporter;

  public KustoKafkaMetricsJmxReporter(MetricRegistry metricRegistry, final String connectorName) {
    this.metricRegistry = metricRegistry;
    this.jmxReporter = createJMXReporter(connectorName);
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  /**
   * This function will internally register all metrics present inside metric registry and will
   * register mbeans to the mbeanserver
   */
  public void start() {
    jmxReporter.start();
  }

  /**
   * This function will stop the JMX reporter and unregister all metrics.
   */
  public void stop() {
    jmxReporter.stop();
  }

  /**
   * This method is called to fetch an object name for all registered metrics. It can be called
   * during registration or unregistration. (Internal implementation of codehale)
   *
   * @param connectorName name of the connector. (From Config)
   * @param jmxDomain JMX Domain
   * @param metricName metric name used while registering the metric. (Check {@link
   *     KustoKafkaMetricsUtil#constructMetricName(String, String, String)})
   * @return Object Name constructed from above three args
   */
  @VisibleForTesting
  static ObjectName getObjectName(String connectorName, String jmxDomain, String metricName) {
    try {
      StringBuilder sb =
          new StringBuilder(jmxDomain).append(":connector=").append(connectorName).append(',');

      // each metric name will be in a form pipeName/subDomain/metricName
      Iterator<String> tokens = Splitter.on("/").split(metricName).iterator();
      // Append PipeName
      sb.append("pipe=").append(tokens.next());

      // Append subDomain
      sb.append(",category=").append(tokens.next());

      // append metric name
      sb.append(",name=").append(tokens.next());

      return new ObjectName(sb.toString());
    } catch (MalformedObjectNameException e) {
      LOGGER.warn("Could not create Object name for MetricName:{}", metricName);
      throw new RuntimeException("Error creating ObjectName for metric", e);
    }
  }

  /**
   * Unregister all Kusto Kafka related metrics from registry
   *
   * @param prefixFilter prefix for removing the filter.
   */
  public void removeMetricsFromRegistry(final String prefixFilter) {
    if (metricRegistry.getMetrics().size() != 0) {
      LOGGER.debug("Unregistering all metrics for pipe:{}", prefixFilter);
      metricRegistry.removeMatching(MetricFilter.startsWith(prefixFilter));
      LOGGER.debug(
          "Metric registry size for pipe:{} is:{}, names:{}",
          prefixFilter,
          metricRegistry.getMetrics().size(),
          metricRegistry.getMetrics().keySet().toString());
    }
  }

  /**
   * Create JMXReporter Instance, which internally handles the mbean server fetching and
   * registration of Mbeans. We use codehale metrics library to achieve this. More details
   * here: @see <a href="https://metrics.dropwizard.io/4.2.0/getting-started.html">DropWizard</a>
   *
   * <p>We will convert all duration to SECONDS and prefix our metrics with {@link
   * KustoKafkaMetricsUtil#JMX_METRIC_PREFIX}
   *
   * @param connectorName connectorName passed inside configuration
   * @return JMXReporter instance.
   */
  private JmxReporter createJMXReporter(final String connectorName) {
    return JmxReporter.forRegistry(this.metricRegistry)
        .inDomain(JMX_METRIC_PREFIX)
        .convertDurationsTo(TimeUnit.SECONDS)
        .createsObjectNamesWith(
            (ignoreMeterType, jmxDomain, metricName) ->
                getObjectName(connectorName, jmxDomain, metricName))
        .build();
  }
}
