#!/bin/bash

# Script to produce sample JSON messages to Kafka
echo "Producing sample JSON messages to test.json.topic..."
echo ""
echo "Sample messages will be sent. Press Ctrl+C to stop."
echo ""

# Produce 10 sample messages
for i in {1..20}; do
    MESSAGE="{\"id\": $i, \"name\": \"test$i\", \"value\": $((100 + i * 10)), \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    echo "$MESSAGE" | /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic test.json.topic
    echo "Sent: $MESSAGE"
done

echo ""
echo "10 messages sent successfully!"
echo ""