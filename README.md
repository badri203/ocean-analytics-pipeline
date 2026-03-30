cat > /workspaces/ocean-analytics-pipeline/README.md << 'EOF'

## Problem Statement

Our oceans cover 71% of Earth's surface and are the primary driver of global climate, weather patterns, and biodiversity. Yet real-time ocean monitoring data is scattered across dozens of agencies, APIs, and research institutions — making it nearly impossible for researchers, conservationists, and policymakers to get a unified view of what's happening in our oceans right now.

This project addresses three interconnected questions:

**1. What are current ocean conditions globally?**
NOAA operates 900+ buoys across every ocean, each reporting wave height, water temperature, wind speed, and pressure every hour. This data exists but is buried in raw text files that require significant effort to access and interpret.

**2. Where are marine mammals being sighted, and is that changing?**
Marine mammal populations are key indicators of ocean ecosystem health. Whale and dolphin sighting data exists through organizations like OBIS, but correlating it with environmental conditions requires joining datasets that were never designed to work together.

**3. How do extreme events like storms and earthquakes affect ocean systems?**
Seismic and storm events directly impact ocean conditions and marine wildlife behavior. Understanding these correlations requires a pipeline that can process multiple data streams simultaneously.

### The Solution

This pipeline ingests live data from NOAA buoys, marine mammal observation databases, and seismic monitoring systems — streams it through a unified GCP data platform — and presents it in an interactive dashboard that answers these questions in real time.

The result: a single dashboard that shows ocean temperature anomalies, marine wildlife activity trends, and environmental event correlations — updated continuously from live data sources.

# Ocean Analytics Pipeline

An end-to-end oceanographic data pipeline built on GCP, streaming live ocean buoy readings and marine wildlife sightings through Pub/Sub, Dataflow, and BigQuery to a Looker Studio dashboard.


## Problem Statement

Our oceans cover 71% of Earth's surface and are the primary driver of global climate, weather patterns, and biodiversity. Yet real-time ocean monitoring data is scattered across dozens of agencies, APIs, and research institutions — making it nearly impossible for researchers, conservationists, and policymakers to get a unified view of what is happening in our oceans right now.

This project addresses three interconnected questions:

**1. What are current ocean conditions globally?**
NOAA operates 900+ buoys across every ocean, each reporting wave height, water temperature, wind speed, and pressure every hour. This data exists but is buried in raw text files that require significant effort to access and interpret.

**2. Where are marine mammals being sighted, and is that changing?**
Marine mammal populations are key indicators of ocean ecosystem health. Whale and dolphin sighting data exists through organizations like OBIS, but correlating it with environmental conditions requires joining datasets that were never designed to work together.

**3. How do extreme events like storms and earthquakes affect ocean systems?**
Seismic and storm events directly impact ocean conditions and marine wildlife behavior. Understanding these correlations requires a pipeline that can process multiple data streams simultaneously.

### The Solution

This pipeline ingests live data from NOAA buoys, marine mammal observation databases, and seismic monitoring systems — streams it through a unified GCP data platform — and presents it in an interactive Looker Studio dashboard that answers these questions in real time.

The result: a single dashboard showing ocean temperature conditions, marine wildlife activity trends, and environmental event correlations — updated continuously from live public data sources with no authentication required.


## Architecture

![Architecture Diagram](dashboard/architecture.png)

### Streaming Pipeline (continuous, 24/7)
```
NOAA NDBC Buoys (hourly) ──┐
OBIS Marine Mammals (30min) ├──→ Pub/Sub (3 topics) ──→ Dataflow (Apache Beam) ──→ GCS + BigQuery
USGS/NHC Storm Events (1hr) ┘
```

### Batch Pipeline (scheduled)
```
ERSST Historical SST (monthly) ──┐
OBIS Species Reference (monthly) ├──→ Airflow DAGs ──→ BigQuery reference tables
NOAA MPA Boundaries (annually)   ┘
NOAA HURDAT2 Storm History (annually)
NDBC Station Metadata (weekly)
```

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

| Source | Type | What it provides | Auth |
|---|---|---|---|
| NOAA NDBC Buoys | Streaming | Wave height, water temp, wind speed, pressure | None |
| OBIS Marine Mammals | Streaming | Live whale, dolphin, seal occurrence records | None |
| USGS/NHC Storm Events | Streaming | Active storm tracks + significant seismic events | None |
| NOAA ERSST | Batch | Historical SST baseline for anomaly calculation | None |
| OBIS Species Reference | Batch | Historical marine species distribution | None |
| NOAA MPA Inventory | Batch | Marine protected area boundaries | None |
| NOAA HURDAT2 | Batch | Historical hurricane tracks since 1851 | None |
| NDBC Station Metadata | Batch | Buoy station reference data | None |

## GCP Infrastructure (Terraform)

| Resource | Name |
|---|---|
| GCS Bucket | `project-a3416167-bd30-4a48-987-ocean-analytics-lake` |
| BigQuery Dataset | `ocean_analytics` |
| Pub/Sub Topic | `buoy-readings-topic` |
| Pub/Sub Topic | `whale-sightings-topic` |
| Pub/Sub Topic | `storm-tracks-topic` |
| Pub/Sub Topic | `ocean-dead-letter-topic` |

## GCS Data Lake Structure
```
gs://project-a3416167-bd30-4a48-987-ocean-analytics-lake/
├── bronze/                    ← raw, immutable, system of record
│   ├── buoy-readings/
│   ├── whale-sightings/
│   └── storm-tracks/
├── silver/                    ← cleaned, validated, enriched
│   ├── buoy-readings/
│   ├── whale-sightings/
│   └── storm-tracks/
├── gold/                      ← aggregated, analytics-ready
└── dataflow/
    ├── staging/
    └── temp/
```

## BigQuery Schema (Star Schema)

### Fact Tables
- `fact_buoy_readings` — partitioned by date, clustered by station_id
- `fact_whale_sightings` — partitioned by date, clustered by species
- `fact_storm_tracks` — partitioned by date, clustered by basin

### Dimension Tables
- `dim_location` — buoy station metadata
- `dim_species` — marine species reference
- `dim_mpa` — marine protected area boundaries

### Reference Tables (loaded by Airflow)
- `ref_ersst_baseline` — historical SST averages for anomaly calculation
- `ref_hurdat2_tracks` — historical hurricane tracks

## Setup

### Prerequisites
- GCP account with billing enabled
- Terraform >= 1.0
- Docker and Docker Compose
- Python 3.12+
- uv (Python package manager)
- gcloud CLI

### 1. Clone the repo
```bash
git clone https://github.com/badri203/ocean-analytics-pipeline.git
cd ocean-analytics-pipeline
```

### 2. Set up GCP credentials
```bash
mkdir -p ~/.gcp
# Copy your service account key to ~/.gcp/ocean-pipeline-runner-sa.json
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/ocean-pipeline-runner-sa.json
```

### 3. Provision GCP Infrastructure
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your GCP project ID
terraform init
terraform plan
terraform apply
```

### 4. Run ingestion scripts
```bash
# Buoy readings
cd ingestion/buoy && uv run python main.py

# Whale sightings
cd ingestion/whale && uv run python main.py

# Storm tracks
cd ingestion/storm && uv run python main.py
```

### 5. Start Airflow
```bash
cd airflow
docker-compose up -d
```

## Project Structure
```
ocean-analytics-pipeline/
├── ingestion/
│   ├── buoy/          ← NOAA NDBC buoy ingestion
│   ├── whale/         ← OBIS marine mammal ingestion
│   └── storm/         ← NHC/USGS storm event ingestion
├── dataflow/          ← Apache Beam streaming pipeline
├── airflow/
│   └── dags/          ← Airflow DAG definitions
├── bigquery/
│   └── schemas/       ← BigQuery table schema definitions
├── terraform/         ← GCP infrastructure as code
├── docker/            ← Dockerfiles
└── dashboard/         ← Looker Studio screenshots
```

## Pipeline Design Decisions

**Why streaming + batch?**
The core pipeline is streaming — live buoy readings and marine mammal occurrences flow continuously through Pub/Sub and Dataflow. Batch processing handles reference data enrichment — historical temperature baselines, species data, MPA boundaries — data that updates monthly or annually.

**Why Pub/Sub?**
Pub/Sub isolates ingestion volatility from downstream processing. If Dataflow slows or restarts, Pub/Sub queues messages — without it, a downstream failure causes permanent data loss since external APIs only serve current data.

**Why GCS + BigQuery?**
GCS is the immutable system of record — raw data stored permanently and cheaply, replayable at any time. BigQuery is the serving layer — fast SQL queries with partitioning and clustering, native Looker Studio connection.

**Why Airflow?**
Airflow provides orchestration for batch reference data loads with retry logic, dependency management, execution history, and a monitoring UI. A cron job would have none of these.

## Note on Whale Hotline API
The original design used the Whale Hotline API (hotline.whalemuseum.org) for live marine mammal sightings. This domain was unreachable from the deployment environment due to network restrictions. The pipeline uses the OBIS API instead, which provides real, validated marine species occurrence data for 8 whale and dolphin species. The ingestion code is structured identically and would work with the Whale Hotline API with zero architectural changes.
EOF