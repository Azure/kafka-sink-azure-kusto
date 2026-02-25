# Kafka Connect image with Kusto Sink Connector and JMX Exporter.
# Based on Strimzi Kafka image for lightweight, production-ready Kafka tooling.
#
# Build context should be the docker/ directory:
#   docker compose build  (from docker/)
#   OR: docker build -t kusto-kafka-connect .  (from docker/)
#
# Before building, build the connector JAR from the repo root:
#   mvn clean package -DskipTests
FROM quay.io/strimzi/kafka:latest-kafka-3.7.0

USER root

# Install the Prometheus JMX Exporter agent
ARG JMX_EXPORTER_VERSION=1.0.1
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /opt/jmx-exporter/jmx_prometheus_javaagent.jar
RUN chmod 644 /opt/jmx-exporter/jmx_prometheus_javaagent.jar

# Copy JMX Exporter configuration
COPY jmx-exporter/kafka-connect-jmx.yaml /opt/jmx-exporter/config.yaml

# Create plugins directory
RUN mkdir -p /opt/kafka/plugins/kusto-sink

# Copy the Kusto Sink Connector JAR (build with: mvn clean package -DskipTests)
# The JAR is located in the repo root's target/ directory
COPY connector-jar/*.jar /opt/kafka/plugins/kusto-sink/kafka-sink-azure-kusto.jar

# Copy Kafka Connect distributed properties
COPY kafka-connect/connect-distributed.properties /opt/kafka/config/connect-distributed.properties

# Copy the connector deployment script
COPY kafka-connect/start-connect.sh /opt/kafka/start-connect.sh
RUN chmod +x /opt/kafka/start-connect.sh

USER 1001

# Kafka Connect REST API
EXPOSE 8083
# JMX Exporter Prometheus metrics endpoint
EXPOSE 9404

CMD ["/opt/kafka/start-connect.sh"]
