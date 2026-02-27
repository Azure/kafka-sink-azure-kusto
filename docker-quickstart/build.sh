#!/bin/bash
# Build the Kafka Connect Docker image with the Kusto Sink Connector.
# Run this from the docker/ directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Building connector JAR..."
cd "${REPO_ROOT}"
mvn clean package -DskipTests -q

echo "Preparing Docker build context..."
cd "${SCRIPT_DIR}"
mkdir -p connector-jar
cp "${REPO_ROOT}"/target/kafka-sink-azure-kusto-*-jar-with-dependencies.jar connector-jar/

echo "Building Docker image..."
docker compose build

echo "Cleaning up..."
rm -rf connector-jar

echo ""
echo "Build complete. Start services with:"
echo "  docker compose up -d"
