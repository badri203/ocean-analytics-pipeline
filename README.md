cat > README.md << 'EOF'
# Ocean Analytics Pipeline

An end-to-end oceanographic data pipeline built on GCP, streaming live ocean buoy readings and marine wildlife sightings through Pub/Sub, Dataflow, and BigQuery to a Looker Studio dashboard.

## Architecture

- **Streaming**: NOAA NDBC buoys, Whale Hotline API, NHC storm tracks → Pub/Sub → Dataflow → BigQuery
- **Batch**: ERSST, OBIS, MPA, HURDAT2, station metadata → Airflow → BigQuery
- **Visualization**: Looker Studio (live BigQuery connection)

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Docker |
| Message Queue | GCP Pub/Sub |
| Stream Processing | GCP Dataflow (Apache Beam) |
| Batch Orchestration | Apache Airflow (Docker Compose) |
| Data Lake | GCP Cloud Storage (Bronze/Silver/Gold) |
| Data Warehouse | GCP BigQuery |
| Dashboard | Looker Studio |
| Infrastructure | Terraform |

## Data Sources

| Source | Type | What it provides |
|---|---|---|
| NOAA NDBC Buoys | Streaming | Ocean readings — wave height, temperature, wind, pressure |
| Whale Hotline API | Streaming | Live marine mammal sightings |
| NHC Storm Tracks | Streaming | Active tropical storm positions |
| NOAA ERSST | Batch | Historical SST baseline for anomaly calculation |
| OBIS | Batch | Historical marine species occurrences |
| NOAA MPA Inventory | Batch | Marine protected area boundaries |
| NOAA HURDAT2 | Batch | Historical hurricane tracks |
| NDBC Station Metadata | Batch | Buoy station reference data |

## Setup

### Prerequisites
- GCP account with billing enabled
- Terraform installed
- Docker and Docker Compose installed
- Python 3.11+
- uv (Python package manager)

### 1. Provision GCP Infrastructure
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your GCP project ID
terraform init
terraform plan
terraform apply
```

### 2. Start Airflow
```bash
cd airflow
docker-compose up -d
```

### 3. Run Ingestion
```bash
cd ingestion
docker-compose up -d
```

## Project Structure
```
ocean-analytics-pipeline/
├── ingestion/          # API polling scripts → Pub/Sub publishers
│   ├── buoy/          # NOAA NDBC buoy ingestion
│   ├── whale/         # Whale Hotline API ingestion
│   └── storm/         # NHC storm track ingestion
├── dataflow/          # Apache Beam streaming pipeline
├── airflow/           # Batch orchestration DAGs
│   └── dags/         # Airflow DAG definitions
├── bigquery/          # Table schemas
│   └── schemas/      # JSON schema definitions
├── terraform/         # GCP infrastructure as code
├── docker/            # Dockerfiles
└── dashboard/         # Looker Studio screenshots
```

## Pipeline Justification

**Why streaming + batch?**
The core pipeline is streaming — live buoy readings and whale sightings flow continuously through Pub/Sub and Dataflow. Batch processing is used exclusively for reference data enrichment — historical temperature baselines, species data, MPA boundaries — data that updates monthly or annually and would be wasteful to stream.

**Why Pub/Sub?**
Pub/Sub isolates ingestion volatility from downstream processing. If Dataflow slows or restarts, Pub/Sub queues messages so no data is lost. External APIs only serve current data — without Pub/Sub, a downstream failure means permanent data loss.

**Why GCS + BigQuery?**
GCS is the immutable system of record — cheap, permanent, replayable. BigQuery is the serving layer — fast SQL queries, partitioning, clustering, native Looker Studio connection.
EOF