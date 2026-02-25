#!/bin/bash
# Deploy the Kusto Sink Connector to Kafka Connect.
# This script reads the connector config template, substitutes environment
# variables from .env, and deploys it via the Kafka Connect REST API.

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

# Load .env file if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Check required variables
required_vars=(KAFKA_TOPIC KUSTO_DATABASE KUSTO_TABLE KUSTO_INGEST_URL KUSTO_ENGINE_URL KUSTO_AUTH_APPID KUSTO_AUTH_APPKEY KUSTO_AUTH_AUTHORITY)
for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: Required variable $var is not set. Please configure your .env file."
        exit 1
    fi
done

# Substitute env vars in the connector config template
CONNECTOR_CONFIG=$(cat connector-config/kusto-sink-connector.json \
    | sed "s|\${KAFKA_TOPIC}|${KAFKA_TOPIC}|g" \
    | sed "s|\${KUSTO_DATABASE}|${KUSTO_DATABASE}|g" \
    | sed "s|\${KUSTO_TABLE}|${KUSTO_TABLE}|g" \
    | sed "s|\${KUSTO_INGEST_URL}|${KUSTO_INGEST_URL}|g" \
    | sed "s|\${KUSTO_ENGINE_URL}|${KUSTO_ENGINE_URL}|g" \
    | sed "s|\${KUSTO_AUTH_APPID}|${KUSTO_AUTH_APPID}|g" \
    | sed "s|\${KUSTO_AUTH_APPKEY}|${KUSTO_AUTH_APPKEY}|g" \
    | sed "s|\${KUSTO_AUTH_AUTHORITY}|${KUSTO_AUTH_AUTHORITY}|g")

echo "Deploying Kusto Sink Connector to ${CONNECT_URL}..."

# Wait for Kafka Connect to be ready
for i in $(seq 1 30); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        break
    fi
    echo "Waiting for Kafka Connect to be ready... (attempt $i/30)"
    sleep 5
done

# Check if connector already exists
if curl -sf "${CONNECT_URL}/connectors/kusto-sink-connector" > /dev/null 2>&1; then
    echo "Connector already exists, updating..."
    CONFIG_ONLY=$(echo "${CONNECTOR_CONFIG}" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['config']))" 2>/dev/null || echo "${CONNECTOR_CONFIG}" | jq '.config')
    curl -sf -X PUT \
        -H "Content-Type: application/json" \
        -d "${CONFIG_ONLY}" \
        "${CONNECT_URL}/connectors/kusto-sink-connector/config" | python3 -m json.tool 2>/dev/null || cat
else
    echo "Creating new connector..."
    curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d "${CONNECTOR_CONFIG}" \
        "${CONNECT_URL}/connectors" | python3 -m json.tool 2>/dev/null || cat
fi

echo ""
echo "Connector deployed. Check status:"
echo "  curl ${CONNECT_URL}/connectors/kusto-sink-connector/status"
echo ""
echo "JMX metrics available at:"
echo "  curl http://localhost:9404/metrics | grep kusto_sink"

# ---------------------------------------------------------------------------
# Produce 100 sample messages to verify the pipeline
# ---------------------------------------------------------------------------
echo ""
echo "Producing 100 sample JSON messages to topic '${KAFKA_TOPIC}'..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"${SCRIPT_DIR}/produce-messages.sh" 100 0.1 &

echo ""
echo "Done! Messages should appear in Kusto within ~30-60 seconds."
echo "Verify in Kusto Web Explorer with:"
echo "  ${KUSTO_TABLE} | take 10"
echo ""
echo "Check connector JMX metrics with:"
echo "  curl -s http://localhost:9404/metrics | grep kusto_sink"
