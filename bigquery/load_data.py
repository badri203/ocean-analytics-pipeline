import json
import logging
import uuid
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "project-a3416167-bd30-4a48-987"
DATASET_ID = "ocean_analytics"
client = bigquery.Client(project=PROJECT_ID)


def create_table(table_id, schema, partition_field, cluster_fields):
    """Create a partitioned and clustered BigQuery table."""
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    table = bigquery.Table(full_table_id, schema=schema)
    
    # Partition by date
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field,
    )
    
    # Cluster for query optimization
    table.clustering_fields = cluster_fields
    
    try:
        table = client.create_table(table, exists_ok=True)
        logger.info(f"Table {table_id} ready — partitioned by {partition_field}, clustered by {cluster_fields}")
    except Exception as e:
        logger.error(f"Failed to create table {table_id}: {e}")
        raise


def load_buoy_data():
    """Fetch and load buoy readings into BigQuery."""
    logger.info("Fetching buoy data from NOAA NDBC...")
    url = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    rows = []
    now = datetime.now(timezone.utc)
    lines = [l for l in response.text.split("\n") if not l.startswith("#") and l.strip()]
    
    for line in lines:
        parts = line.split()
        if len(parts) < 14:
            continue
        try:
            row = {
                "reading_id": str(uuid.uuid4()),
                "station_id": parts[0],
                "latitude": float(parts[1]),
                "longitude": float(parts[2]),
                "timestamp": now.isoformat(),
                "wind_direction_deg": None if parts[3] == "MM" else float(parts[3]),
                "wind_speed_ms": None if parts[4] == "MM" else float(parts[4]),
                "wind_gust_ms": None if parts[5] == "MM" else float(parts[5]),
                "wave_height_m": None if parts[6] == "MM" else float(parts[6]),
                "dominant_wave_period_s": None if parts[7] == "MM" else float(parts[7]),
                "avg_wave_period_s": None if parts[8] == "MM" else float(parts[8]),
                "wave_direction_deg": None if parts[9] == "MM" else float(parts[9]),
                "sea_level_pressure_hpa": None if parts[10] == "MM" else float(parts[10]),
                "air_temp_c": None if parts[12] == "MM" else float(parts[12]),
                "water_temp_c": None if parts[13] == "MM" else float(parts[13]),
                "dewpoint_temp_c": None if parts[14] == "MM" else float(parts[14]) if len(parts) > 14 else None,
                "visibility_nmi": None,
                "tide_ft": None,
                "ingested_at": now.isoformat(),
                "source": "noaa_ndbc",
                "date": now.date().isoformat(),
            }
            rows.append(row)
        except (ValueError, IndexError):
            continue
    
    logger.info(f"Parsed {len(rows)} buoy readings")
    return rows


def load_whale_data():
    """Fetch and load whale sightings into BigQuery."""
    logger.info("Fetching whale data from OBIS...")
    
    species_map = {
        137092: "orca",
        137091: "humpback whale",
        137090: "gray whale",
        137119: "minke whale",
        137098: "sperm whale",
        137094: "blue whale",
        137096: "fin whale",
    }
    
    rows = []
    now = datetime.now(timezone.utc)
    
    for taxon_id, species_name in species_map.items():
        try:
            response = requests.get(
                "https://api.obis.org/v3/occurrence",
                params={"taxonid": taxon_id, "size": 50, "after": "2020-01-01"},
                timeout=30
            )
            response.raise_for_status()
            results = response.json().get("results", [])
            
            for r in results:
                if not r.get("decimalLatitude") or not r.get("decimalLongitude"):
                    continue
                    
                # Parse sighted_at date
                event_date = r.get("eventDate", now.isoformat())
                try:
                    if isinstance(event_date, str) and len(event_date) >= 10:
                        date_part = event_date[:10]
                    else:
                        date_part = now.date().isoformat()
                except:
                    date_part = now.date().isoformat()

                # Fix sighted_at format — ensure full timestamp
                if event_date and len(event_date) == 10:
                    sighted_at_val = event_date + "T00:00:00+00:00"
                else:
                    sighted_at_val = event_date if event_date else now.isoformat()

                # Skip records older than 10 years (BigQuery streaming limit)
                try:
                    from datetime import timedelta
                    cutoff = (now - timedelta(days=3650)).date().isoformat()
                    if date_part < cutoff:
                        continue
                except:
                    pass

                row = {
                    "sighting_id": str(r.get("id", uuid.uuid4())),
                    "species": species_name,
                    "scientific_name": r.get("scientificName", ""),
                    "latitude": r.get("decimalLatitude"),
                    "longitude": r.get("decimalLongitude"),
                    "count": r.get("individualCount", 1) or 1,
                    "depth_m": r.get("depth"),
                    "sighted_at": sighted_at_val,
                    "dataset": r.get("datasetName", "OBIS"),
                    "country": r.get("country"),
                    "locality": r.get("locality"),
                    "ingested_at": now.isoformat(),
                    "source": "obis",
                    "date": date_part,
                }
                rows.append(row)
            logger.info(f"Fetched {len(results)} records for {species_name}")
        except Exception as e:
            logger.error(f"Failed to fetch {species_name}: {e}")
            continue
    
    logger.info(f"Total whale sightings: {len(rows)}")
    return rows


def load_storm_data():
    """Fetch and load storm/seismic data into BigQuery."""
    logger.info("Fetching storm/seismic data from USGS...")
    rows = []
    now = datetime.now(timezone.utc)
    
    try:
        response = requests.get(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson",
            timeout=30
        )
        response.raise_for_status()
        features = response.json().get("features", [])
        
        for f in features:
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [None, None, None])
            if not coords[0] or not coords[1]:
                continue
            
            row = {
                "storm_id": f.get("id", str(uuid.uuid4())),
                "name": props.get("place", "Unknown"),
                "basin": "pacific" if coords[0] < -90 else "atlantic",
                "storm_type": "earthquake",
                "latitude": coords[1],
                "longitude": coords[0],
                "wind_speed_mph": None,
                "pressure_mb": None,
                "magnitude": props.get("mag"),
                "depth_km": coords[2],
                "observed_at": datetime.fromtimestamp(
                    props.get("time", 0) / 1000, tz=timezone.utc
                ).isoformat(),
                "ingested_at": now.isoformat(),
                "source": "usgs_earthquake",
                "date": now.date().isoformat(),
            }
            rows.append(row)
        logger.info(f"Fetched {len(rows)} seismic events")
    except Exception as e:
        logger.error(f"Failed to fetch storm data: {e}")
    
    return rows


def insert_rows(table_id, rows):
    """Insert rows into BigQuery table."""
    if not rows:
        logger.warning(f"No rows to insert for {table_id}")
        return
    
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    errors = client.insert_rows_json(full_table_id, rows)
    
    if errors:
        logger.error(f"Errors inserting into {table_id}: {errors[:3]}")
    else:
        logger.info(f"Successfully inserted {len(rows)} rows into {table_id}")


def main():
    logger.info("Starting BigQuery data load...")
    
    # Create tables
    create_table(
        "fact_buoy_readings",
        schema=[
            bigquery.SchemaField("reading_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("wind_direction_deg", "FLOAT64"),
            bigquery.SchemaField("wind_speed_ms", "FLOAT64"),
            bigquery.SchemaField("wind_gust_ms", "FLOAT64"),
            bigquery.SchemaField("wave_height_m", "FLOAT64"),
            bigquery.SchemaField("dominant_wave_period_s", "FLOAT64"),
            bigquery.SchemaField("avg_wave_period_s", "FLOAT64"),
            bigquery.SchemaField("wave_direction_deg", "FLOAT64"),
            bigquery.SchemaField("sea_level_pressure_hpa", "FLOAT64"),
            bigquery.SchemaField("air_temp_c", "FLOAT64"),
            bigquery.SchemaField("water_temp_c", "FLOAT64"),
            bigquery.SchemaField("dewpoint_temp_c", "FLOAT64"),
            bigquery.SchemaField("visibility_nmi", "FLOAT64"),
            bigquery.SchemaField("tide_ft", "FLOAT64"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        ],
        partition_field="date",
        cluster_fields=["station_id", "source"],
    )
    
    create_table(
        "fact_whale_sightings",
        schema=[
            bigquery.SchemaField("sighting_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("species", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scientific_name", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("count", "INT64"),
            bigquery.SchemaField("depth_m", "FLOAT64"),
            bigquery.SchemaField("sighted_at", "TIMESTAMP"),
            bigquery.SchemaField("dataset", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("locality", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        ],
        partition_field="date",
        cluster_fields=["species", "source"],
    )
    
    create_table(
        "fact_storm_tracks",
        schema=[
            bigquery.SchemaField("storm_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("basin", "STRING"),
            bigquery.SchemaField("storm_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("wind_speed_mph", "FLOAT64"),
            bigquery.SchemaField("pressure_mb", "FLOAT64"),
            bigquery.SchemaField("magnitude", "FLOAT64"),
            bigquery.SchemaField("depth_km", "FLOAT64"),
            bigquery.SchemaField("observed_at", "TIMESTAMP"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        ],
        partition_field="date",
        cluster_fields=["basin", "storm_type"],
    )
    
    # Load data
    buoy_rows = load_buoy_data()
    insert_rows("fact_buoy_readings", buoy_rows)
    
    whale_rows = load_whale_data()
    insert_rows("fact_whale_sightings", whale_rows)
    
    storm_rows = load_storm_data()
    insert_rows("fact_storm_tracks", storm_rows)
    
    logger.info("Data load complete!")


if __name__ == "__main__":
    main()
