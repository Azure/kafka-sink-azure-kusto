# Docker Quickstart: Kusto Sink Connector with JMX Metrics, OTEL & Grafana

This quickstart sets up a complete local environment for the Azure Data Explorer (Kusto)
Kafka Connect Sink Connector with JMX metrics collection, export to Kusto via the
OpenTelemetry Collector, and a Grafana dashboard for visualization.

## Architecture

```
┌──────────────┐    ┌──────────────────────────────────┐    ┌──────────────────┐
│              │    │  Kafka Connect                    │    │  Azure Data      │
│  Kafka       │───>│  + Kusto Sink Connector           │───>│  Explorer        │
│  (Strimzi    │    │  + Prometheus JMX Exporter (:9404)│    │  (data)          │
│   KRaft)     │    └──────────┬───────────────────────┘    └──────────────────┘
│              │               │                                     ^
└──────────────┘               │ Prometheus scrape                   │
                               v                                     │
                    ┌──────────────────────┐            ┌────────────┴─────┐
                    │  OTEL Collector      │            │  Azure Data      │
                    │  (Contrib)           │───────────>│  Explorer        │
                    │  prometheus receiver │            │  (metrics)       │
                    │  + ADX exporter      │            │                  │
                    └──────────────────────┘            └────────────┬─────┘
                                                                    │
                                                        ┌───────────┴──────┐
                                                        │  Grafana (:3000) │
                                                        │  ADX plugin      │
                                                        │  + Dashboard     │
                                                        └──────────────────┘
```

### Components

| Service | Image | Description | Ports |
|---|---|---|---|
| `kafka` | Custom (Strimzi-based) | Kafka broker in KRaft mode + JMX Exporter | 29092, 9405 |
| `kafka-connect` | Custom (Strimzi-based) | Kafka Connect with Kusto connector + JMX Exporter | 8083, 9404 |
| `otel-collector` | `otel/opentelemetry-collector-contrib` | Scrapes JMX metrics, exports to Kusto | — |
| `grafana` | `grafana/grafana` | Dashboard for Kusto Sink metrics via ADX plugin | 3000 |

### JMX Metrics Exposed

The Kusto Sink Connector publishes the following metrics via the JMX MBean
`com.microsoft.azure.kusto.kafka.connect.sink:type=KustoSinkMetrics`:

| Metric | Description |
|---|---|
| `kusto_sink_recordswritten_total` | Total records successfully written to staging files |
| `kusto_sink_recordsfailed_total` | Total records that failed during write |
| `kusto_sink_ingestionattempts_total` | Total file ingestion attempts to Azure Data Explorer |
| `kusto_sink_ingestionsuccesses_total` | Total successful file ingestions |
| `kusto_sink_ingestionfailures_total` | Total failed file ingestions |
| `kusto_sink_dlqrecordssent_total` | Total records sent to the dead letter queue |

## Prerequisites

- **Docker** and **Docker Compose** (v2)
- **Java 17+** and **Maven** (to build the connector JAR)
- An **Azure Data Explorer cluster** with:
  - A database and table for sink data
  - A database for OTEL metrics (can be the same)
  - An Azure AD Service Principal with ingest permissions

## Quick Start

### 1. Build the Connector JAR

From the repository root (or skip if using `build.sh` in step 3):

```bash
mvn clean package -DskipTests
```

This produces `target/kafka-sink-azure-kusto-*-jar-with-dependencies.jar`.

### 2. Configure Environment Variables

```bash
cd docker
cp .env.example .env
# Edit .env with your Azure Data Explorer credentials
```

Fill in all values in `.env`. At minimum you need:

| Variable | Description |
|---|---|
| `KUSTO_INGEST_URL` | Kusto ingestion endpoint |
| `KUSTO_ENGINE_URL` | Kusto engine/query endpoint (also used by OTEL and Grafana) |
| `KUSTO_DATABASE` | Target database for data and metrics |
| `KUSTO_TABLE` | Target table for Kafka data |
| `KUSTO_METRICS_TABLE` | Target table for OTEL metrics (default: `OTELMetrics`) |
| `KUSTO_AUTH_APPID` | Azure AD app ID (shared by all components) |
| `KUSTO_AUTH_APPKEY` | Azure AD app secret |
| `KUSTO_AUTH_AUTHORITY` | Azure AD tenant ID |

### 3. Create the Kusto Table

Before starting the connector, create the target table and JSON mapping in your
Azure Data Explorer database. Open [Kusto Web Explorer](https://dataexplorer.azure.com)
and run the commands in `connector-config/create-table.kql`:

```kql
// Create the target table
.create table KafkaSinkQuickstart (
    id: int,
    timestamp: datetime,
    sensor: string,
    value: int,
    unit: string,
    message_number: int
)

// Create a JSON ingestion mapping
.create table KafkaSinkQuickstart ingestion json mapping 'quickstart_json_mapping'
  '['
  '  {"column": "id",             "path": "$.id",             "datatype": "int"     },'
  '  {"column": "timestamp",      "path": "$.timestamp",      "datatype": "datetime"},'
  '  {"column": "sensor",         "path": "$.sensor",         "datatype": "string"  },'
  '  {"column": "value",          "path": "$.value",          "datatype": "int"     },'
  '  {"column": "unit",           "path": "$.unit",           "datatype": "string"  },'
  '  {"column": "message_number", "path": "$.message_number", "datatype": "int"     }'
  ']'

// Create the OTEL metrics table (required for JMX metrics export via OTEL Collector)
.create-merge table OTELMetrics (
    Timestamp: datetime,
    MetricName: string,
    MetricType: string,
    MetricUnit: string,
    MetricDescription: string,
    MetricValue: real,
    Host: string,
    ResourceAttributes: dynamic,
    MetricAttributes: dynamic
)
```

> **Important:** `OTEL_KUSTO_CLUSTER_URL` in your `.env` must be the **engine URL**
> (e.g. `https://cluster.region.kusto.windows.net`), **not** the ingest URL. The
> OTEL exporter resolves the ingestion endpoint automatically.

### 4. Build and Start Services

```bash
# Build the connector JAR and Docker image in one step
./build.sh
```

Or manually:

```bash
# From repo root: build the JAR
cd .. && mvn clean package -DskipTests && cd docker

# Copy JAR to build context, build image, clean up
mkdir -p connector-jar
cp ../target/kafka-sink-azure-kusto-*-jar-with-dependencies.jar connector-jar/
docker compose build
rm -rf connector-jar
```

Start the services:

```bash
docker compose up -d
```

Wait for all services to be healthy:

```bash
docker compose ps
```

### 5. Deploy the Connector and Produce Sample Data

```bash
./deploy-connector.sh
```

This reads `connector-config/kusto-sink-connector.json`, substitutes your `.env` values,
deploys it via the Kafka Connect REST API, and then **automatically produces 100 sample
JSON messages** to verify the pipeline end-to-end.

To produce more messages later:

```bash
./produce-messages.sh          # 1 msg/sec, runs forever
./produce-messages.sh 500 0.1  # 500 messages, 10/sec
```

### 6. Verify

**Check connector status:**

```bash
curl http://localhost:8083/connectors/kusto-sink-connector/status | jq .
```

**View JMX metrics (Prometheus format):**

```bash
curl -s http://localhost:9404/metrics | grep kusto_sink
```

Expected output:

```
kusto_sink_recordswritten_total 100.0
kusto_sink_recordsfailed_total 0.0
kusto_sink_ingestionattempts_total 6.0
kusto_sink_ingestionsuccesses_total 6.0
kusto_sink_ingestionfailures_total 0.0
kusto_sink_dlqrecordssent_total 0.0
```

**Produce test messages:**

```bash
# Continuously produce 1 JSON message per second
./produce-messages.sh

# Send exactly 50 messages
./produce-messages.sh 50

# Send messages every 0.2 seconds (5/sec), forever
./produce-messages.sh 0 0.2
```

Sample message format:

```json
{"id":4821,"timestamp":"2026-02-25T06:15:00Z","sensor":"sensor-3","value":472,"unit":"celsius","message_number":1}
```

After the flush interval, check that metrics are incrementing:

```bash
curl -s http://localhost:9404/metrics | grep kusto_sink_recordswritten
```

### 7. Query Ingested Data in Kusto

After the flush interval (~30 seconds) and Kusto batching policy (~30 seconds), your
data should appear. Open [Kusto Web Explorer](https://dataexplorer.azure.com) and run:

```kql
KafkaSinkQuickstart
| take 10
```

```kql
KafkaSinkQuickstart
| summarize count(), avg(value), min(value), max(value) by sensor
```

### 8. Query Metrics in Kusto

After the OTEL Collector exports metrics (every ~15 seconds), you can query them in Kusto:

```kql
OTELMetrics
| where MetricName startswith "kusto_sink"
| project Timestamp, MetricName, MetricValue
| order by Timestamp desc
| take 50
```

### 9. View Grafana Dashboard

Grafana starts automatically with a pre-provisioned **Kafka Connect Kusto Sink Metrics**
dashboard.

1. Open [http://localhost:3000](http://localhost:3000) in your browser
2. Login with `admin` / `admin` (skip password change for quickstart)
3. Navigate to **Dashboards → Kafka Connect → Kafka Connect Kusto Sink Metrics**

The dashboard includes:

| Panel | Description |
|---|---|
| Records Written | Time series of total records written to Kusto |
| Records Failed | Time series of failed records |
| Ingestion Attempts/Successes/Failures | Combined ingestion lifecycle chart |
| DLQ Records Sent | Dead letter queue records over time |
| Status tiles | Current values for all key metrics + task running status |
| Offset Commit Completion Rate | Kafka Connect sink task offset commit rate |
| All Kusto Sink Metrics | Table view of latest values for all metrics |

> **Note:** The Grafana ADX datasource is auto-configured using the same credentials
> as the OTEL Collector (`KUSTO_*` variables from `.env`). The Service Principal
> needs at least **Viewer** role on the metrics database.

### 10. Import Kusto Web Explorer Dashboard

A pre-built Azure Data Explorer dashboard is provided for use directly in
[Kusto Web Explorer](https://dataexplorer.azure.com) — no Grafana required.

**Two pages:**
- **Kusto Sink Connector** — Records written/failed, ingestion lifecycle, DLQ, task status, success rate
- **Kafka Broker** — Messages in/sec, bytes in/out, log offsets, log size, broker health, request latency

**To import:**

1. Open [Kusto Web Explorer](https://dataexplorer.azure.com)
2. Go to **Dashboards** → **New dashboard** → **Import dashboard from file**
3. Select `dashboards/kusto-dashboard.json` from this quickstart
4. After import, go to **Data sources** → edit the **"Kusto Metrics"** data source:
   - Replace **Cluster URI** (`https://CLUSTER.REGION.kusto.windows.net`) with your actual cluster
   - Replace **Database** (`otelmetrics`) with your metrics database name
5. Click **Save** and the dashboard tiles will populate with your metrics

> The dashboard uses a `_startTime` / `_endTime` time range parameter with
> auto-refresh every 1 minute.

## Customization

### Connector Configuration

Edit `connector-config/kusto-sink-connector.json` to change:
- Data format (`json`, `csv`, `avro`)
- Flush size and interval
- Error handling behavior
- Multiple topic-to-table mappings

### JMX Exporter Rules

Edit `jmx-exporter/kafka-connect-jmx.yaml` to add or modify the Prometheus
metric export rules. The default config exports both Kusto Sink and standard
Kafka Connect metrics.

### OTEL Collector

Edit `otel-collector/otel-collector-config.yaml` to:
- Change the scrape interval
- Add additional exporters (e.g., `otlp` for Grafana, `prometheus` for self-scraping)
- Add processors for filtering or transforming metrics

## Stopping

```bash
docker compose down
```

To also remove volumes and networks:

```bash
docker compose down -v
```

## Troubleshooting

**Kafka Connect won't start:**
```bash
docker compose logs kafka-connect
```

**Connector in FAILED state:**
```bash
curl http://localhost:8083/connectors/kusto-sink-connector/status | jq .
```

**No metrics visible:**
```bash
# Check JMX Exporter is running
curl http://localhost:9404/metrics | head -20

# Check OTEL Collector logs
docker compose logs otel-collector
```

**Authentication errors:**
Verify your Azure AD Service Principal has `Ingestor` role on the target database.

## File Structure

```
docker/
├── .env.example                              # Environment variable template
├── build.sh                                  # Build connector JAR + Docker image
├── deploy-connector.sh                       # Connector deployment script
├── produce-messages.sh                       # Continuous JSON message producer
├── docker-compose.yml                        # Service orchestration
├── Dockerfile                                # Kafka Connect custom image
├── Dockerfile.kafka                          # Kafka broker image with JMX Exporter
├── README.md                                 # This file
├── connector-config/
│   ├── create-table.kql                      # Kusto table and mapping setup script
│   └── kusto-sink-connector.json             # Connector configuration template
├── dashboards/
│   └── kusto-dashboard.json                  # Azure Data Explorer dashboard (import to Kusto Web Explorer)
├── grafana/
│   ├── dashboards/
│   │   └── kafka-connect-metrics.json        # Pre-built Grafana dashboard
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboard.yaml                # Dashboard provisioning config
│       └── datasources/
│           └── kusto.yaml                    # ADX datasource provisioning config
├── jmx-exporter/
│   ├── kafka-broker-jmx.yaml                # Kafka broker JMX Exporter rules
│   └── kafka-connect-jmx.yaml               # Kafka Connect JMX Exporter rules
├── kafka-connect/
│   ├── connect-distributed.properties        # Kafka Connect worker config
│   └── start-connect.sh                      # Entrypoint script with JMX agent
└── otel-collector/
    └── otel-collector-config.yaml            # OTEL Collector pipeline config
```
