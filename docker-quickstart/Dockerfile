# Kafka Connect image with Kusto Sink Connector and JMX Exporter.
# Based on the Microsoft-approved OpenJDK image (MCR) with Apache Kafka installed into /opt/kafka.
#
# Build context should be the docker/ directory:
#   docker compose build  (from docker/)
#   OR: docker build -t kusto-kafka-connect .  (from docker/)
#
# Before building, build the connector JAR from the repo root:
#   mvn clean package -DskipTests
FROM mcr.microsoft.com/openjdk/jdk:17-ubuntu@sha256:deada39c65df1e55d0b711ba9006438df2681d6289fb87e016c77dde09b5654a

USER root

ARG KAFKA_VERSION=3.7.0
ARG SCALA_VERSION=2.13
ARG KAFKA_SHA512=b8679283a2d8dab86e7c636b2c688fe9d9e64ac437241f65ef7a1733f4d26a2bd415eefa04f09f1911373bcd2a5dbc3838c76347f68656425c09202cd290ce91

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /tmp/kafka.tgz "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && echo "${KAFKA_SHA512}  /tmp/kafka.tgz" | sha512sum -c - \
    && mkdir -p /opt/kafka \
    && tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 \
    && rm /tmp/kafka.tgz \
    && groupadd -g 1001 kafka \
    && useradd -u 1001 -g 1001 -d /opt/kafka -s /bin/bash kafka

WORKDIR /opt/kafka

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
RUN chmod +x /opt/kafka/start-connect.sh \
    && chown -R 1001:1001 /opt/kafka

USER 1001

# Kafka Connect REST API
EXPOSE 8083
# JMX Exporter Prometheus metrics endpoint
EXPOSE 9404

CMD ["/opt/kafka/start-connect.sh"]
