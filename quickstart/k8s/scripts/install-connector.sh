#!/bin/bash
echo "Install script started"
confluent-hub install microsoftcorporation/kafka-sink-azure-kusto:4.0.1 --no-prompt --verbose --component-dir /usr/share/confluent-hub-components --worker-configs /usr/local/etc/kafka/connect-distributed.properties
echo "ADX connector installed with exit code : '$?'"