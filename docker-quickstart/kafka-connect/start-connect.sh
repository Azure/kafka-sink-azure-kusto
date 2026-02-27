#!/bin/bash
# Start Kafka Connect in distributed mode with JMX Exporter agent.

export KAFKA_OPTS="-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/config.yaml"

echo "Starting Kafka Connect with JMX metrics exporter on port 9404..."
exec /opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties
