#!/bin/bash
# Produce sample JSON messages to the Kafka topic.
# Uses kafka-console-producer.sh inside the running kafka container.
#
# Usage:
#   ./produce-messages.sh              # default: 100 messages, parallel batch
#   ./produce-messages.sh 1000         # send 1000 messages
#   ./produce-messages.sh 100 1        # send 100 messages, 1 msg/sec (serial mode)

set -euo pipefail

MAX_MESSAGES="${1:-100}"
INTERVAL="${2:-0}"

# Load topic name from .env
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi
TOPIC="${KAFKA_TOPIC:-kusto-sink-topic}"

# Generate a batch of JSON messages
generate_batch() {
    local start=$1
    local count=$2
    for i in $(seq "$start" "$((start + count - 1))"); do
        ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        id=$((RANDOM % 10000))
        value=$((RANDOM % 1000))
        sensor="sensor-$((RANDOM % 5 + 1))"
        echo "{\"id\":${id},\"timestamp\":\"${ts}\",\"sensor\":\"${sensor}\",\"value\":${value},\"unit\":\"celsius\",\"message_number\":${i}}"
    done
}

if [ "${INTERVAL}" = "0" ]; then
    # Parallel batch mode: generate all messages and pipe them in one shot
    echo "Producing ${MAX_MESSAGES} JSON messages to topic '${TOPIC}' (batch mode)..."
    BATCH_SIZE=500
    sent=0
    while [ "$sent" -lt "$MAX_MESSAGES" ]; do
        remaining=$((MAX_MESSAGES - sent))
        chunk=$((remaining < BATCH_SIZE ? remaining : BATCH_SIZE))
        generate_batch $((sent + 1)) "$chunk" | \
            docker exec -i -e KAFKA_OPTS="" kafka bin/kafka-console-producer.sh \
                --bootstrap-server localhost:9092 \
                --topic "${TOPIC}" 2>/dev/null
        sent=$((sent + chunk))
        echo "  Sent ${sent}/${MAX_MESSAGES} messages"
    done
    echo "Done. Sent ${MAX_MESSAGES} messages to '${TOPIC}'."
else
    # Serial mode: one message at a time with interval
    echo "Producing JSON messages to topic '${TOPIC}' (interval: ${INTERVAL}s, max: ${MAX_MESSAGES})..."
    echo "Press Ctrl+C to stop."
    echo ""
    for count in $(seq 1 "$MAX_MESSAGES"); do
        ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        id=$((RANDOM % 10000))
        value=$((RANDOM % 1000))
        sensor="sensor-$((RANDOM % 5 + 1))"
        msg="{\"id\":${id},\"timestamp\":\"${ts}\",\"sensor\":\"${sensor}\",\"value\":${value},\"unit\":\"celsius\",\"message_number\":${count}}"

        echo "${msg}" | docker exec -i -e KAFKA_OPTS="" kafka bin/kafka-console-producer.sh \
            --bootstrap-server localhost:9092 \
            --topic "${TOPIC}" 2>/dev/null

        echo "[${count}] ${msg}"
        sleep "${INTERVAL}"
    done
    echo ""
    echo "Done. Sent ${MAX_MESSAGES} messages."
fi
