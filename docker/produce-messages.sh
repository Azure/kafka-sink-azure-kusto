#!/bin/bash
# Continuously produce sample JSON messages to the Kafka topic.
# Uses kafka-console-producer.sh inside the running kafka container.
#
# Usage:
#   ./produce-messages.sh              # default: 1 msg/sec, forever
#   ./produce-messages.sh 100          # send 100 messages then stop
#   ./produce-messages.sh 0 0.5        # forever, 1 msg every 0.5s

set -euo pipefail

MAX_MESSAGES="${1:-0}"
INTERVAL="${2:-1}"

# Load topic name from .env
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi
TOPIC="${KAFKA_TOPIC:-kusto-sink-topic}"

echo "Producing JSON messages to topic '${TOPIC}' (interval: ${INTERVAL}s, max: ${MAX_MESSAGES:-unlimited})..."
echo "Press Ctrl+C to stop."
echo ""

count=0
while true; do
    count=$((count + 1))
    ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    id=$((RANDOM % 10000))
    value=$((RANDOM % 1000))
    sensor="sensor-$((RANDOM % 5 + 1))"
    msg="{\"id\":${id},\"timestamp\":\"${ts}\",\"sensor\":\"${sensor}\",\"value\":${value},\"unit\":\"celsius\",\"message_number\":${count}}"

    echo "${msg}" | docker exec -i -e KAFKA_OPTS="" kafka bin/kafka-console-producer.sh \
        --bootstrap-server localhost:9092 \
        --topic "${TOPIC}" 2>/dev/null

    echo "[${count}] ${msg}"

    if [ "${MAX_MESSAGES}" -gt 0 ] && [ "${count}" -ge "${MAX_MESSAGES}" ]; then
        echo ""
        echo "Done. Sent ${count} messages."
        exit 0
    fi

    sleep "${INTERVAL}"
done
