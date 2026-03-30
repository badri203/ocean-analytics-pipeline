# Ocean Analytics Pipeline

An end-to-end oceanographic data pipeline built on GCP, streaming live ocean buoy readings and marine wildlife sightings through Pub/Sub, Dataflow, and BigQuery to a Looker Studio dashboard.

## Problem Statement

Ocean monitoring data is fragmented across multiple systems, making it difficult to analyze environmental conditions, marine life activity, and extreme events together in real time. This limits our ability to detect patterns, understand ecosystem changes, and respond quickly to emerging risks.

This project builds a **real-time environmental intelligence system** on Google Cloud Platform that ingests and unifies ocean buoy sensor data, marine wildlife sightings, and seismic events — enabling researchers and policymakers to identify emerging ocean anomalies, correlate environmental conditions with marine life behavior, and respond to ecosystem changes in near real time.

### The Core Questions This System Answers

**1. What are current ocean conditions globally?**
NOAA operates 900+ buoys across every ocean, each reporting wave height, water temperature, wind speed, and pressure every hour. This data exists but is buried in raw text files across dozens of endpoints. This system unifies it into a queryable, continuously updated warehouse.

**2. Where are marine mammals being sighted, and is that changing?**
Marine mammal populations are key indicators of ocean ecosystem health. This system correlates whale and dolphin sighting data from OBIS with live buoy readings — enabling questions like: *"Do humpback whale sightings increase when Pacific surface temperatures rise above a certain threshold?"*

**3. How do extreme events affect ocean systems and marine life?**
Seismic and storm events directly impact ocean conditions and marine wildlife behavior. By streaming these events alongside buoy readings, the system can detect correlations between environmental volatility and changes in wildlife activity patterns.

### A Concrete Example

The system detects rising ocean temperatures across North Atlantic buoy stations, correlates them with an increase in humpback whale sightings in that region, and surfaces both signals simultaneously in the dashboard — giving researchers an early indicator of shifting migration patterns driven by warming seas.

### The Solution

This project builds an end-to-end streaming data pipeline on GCP that:
- Continuously ingests live data from 3 public APIs (no authentication required)
- Streams events through Pub/Sub and Dataflow into a Bronze/Silver/Gold data lake
- Loads enrichment reference data via Airflow into a partitioned BigQuery warehouse
- Presents correlated insights through an interactive Looker Studio dashboard

The result is not just a data pipeline — it is a **decision-support system** for ocean environmental intelligence, updated in near real time from live public data sources.

## Architecture

<img width="569" height="450" alt="image" src="https://github.com/user-attachments/assets/9616b663-fa2f-4d0b-b10e-5c719217d992" />


### Streaming Pipeline (continuous, 24/7)
```
NOAA NDBC Buoys (hourly)  ──┐
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
| USGS/NHC Storm Events | Streaming | Active storm tracks and significant seismic events | None |
| NOAA ERSST | Batch | Historical SST baseline for anomaly calculation | None |
| OBIS Species Reference | Batch | Historical marine species distribution | None |
| NOAA MPA Inventory | Batch | Marine protected area boundaries | None |
| NOAA HURDAT2 | Batch | Historical hurricane tracks since 1851 | None |
| NDBC Station Metadata | Batch | Buoy station reference data | None |

## GCP Infrastructure (Terraform)

| Resource | Name |
|---|---|
| GCS Bucket | project-a3416167-bd30-4a48-987-ocean-analytics-lake |
| BigQuery Dataset | ocean_analytics |
| Pub/Sub Topic | buoy-readings-topic |
| Pub/Sub Topic | whale-sightings-topic |
| Pub/Sub Topic | storm-tracks-topic |
| Pub/Sub Topic | ocean-dead-letter-topic |

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
- fact_buoy_readings — partitioned by date, clustered by station_id and source
- fact_whale_sightings — partitioned by date, clustered by species and source
- fact_storm_tracks — partitioned by date, clustered by basin and storm_type

### Dimension Tables
- dim_location — buoy station metadata
- dim_species — marine species reference

### Views (Transformations)
- vw_buoy_daily_summary — daily aggregations per buoy station
- vw_whale_sightings_by_species — sighting counts and totals per species
- vw_whale_sightings_timeline — monthly sighting trends over time
- vw_ocean_temperature_summary — global temperature aggregations
- vw_storm_events_summary — storm and seismic event summaries

## Dashboard

Live Looker Studio dashboard with 4 tiles:
1. KPI Scorecards — active buoys, avg ocean temp, whale sightings, avg wave height
2. Bar chart — marine mammal sightings by species
3. Pie chart — whale sighting species distribution
4. Line chart — historical whale sightings over time

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

### 5. Run Dataflow pipeline
```bash
cd dataflow
pip install apache-beam[gcp]
python pipeline.py --runner DirectRunner
```

### 6. Start Airflow
```bash
cd airflow
docker-compose up -d
```

### 7. Load initial data into BigQuery
```bash
python bigquery/load_data.py
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

## Dashboard
https://lookerstudio.google.com/reporting/79f0c287-496e-44b7-827c-7776e135df8d

## Pipeline Design Decisions

**Why streaming + batch?**
The core pipeline is streaming — live buoy readings and marine mammal occurrences flow continuously through Pub/Sub and Dataflow. Batch processing handles reference data enrichment — historical temperature baselines, species data, MPA boundaries — data that updates monthly or annually.

**Why Pub/Sub?**
Pub/Sub isolates ingestion volatility from downstream processing. If Dataflow slows or restarts, Pub/Sub queues messages — without it, a downstream failure causes permanent data loss since external APIs only serve current data.

**Why GCS + BigQuery?**
GCS is the immutable system of record — raw data stored permanently and cheaply, replayable at any time. BigQuery is the serving layer — fast SQL queries with partitioning and clustering, native Looker Studio connection.

**Why Airflow?**
Airflow provides orchestration for batch reference data loads with retry logic, dependency management, execution history, and a monitoring UI. In production this would migrate to Cloud Composer with zero DAG code changes.

## Note on Whale Hotline API
The original design used the Whale Hotline API for live marine mammal sightings. This domain was unreachable from the deployment environment due to network restrictions. The pipeline uses the OBIS API instead, which provides real, validated marine species occurrence data for 8 whale and dolphin species. The ingestion code is structured identically and would work with the Whale Hotline API with zero architectural changes.
