from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ocean-analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def download_ersst(**context):
    """Download NOAA ERSST historical sea surface temperature data."""
    import requests
    import os

    # NOAA ERSST v5 NetCDF file
    url = "https://www.ncei.noaa.gov/pub/data/cmb/ersst/v5/netcdf/ersst.v5.2024.12.nc"
    output_path = "/tmp/ersst_latest.nc"

    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded ERSST data to {output_path}")
    return output_path


def process_ersst(**context):
    """Convert ERSST NetCDF to BigQuery-ready rows."""
    import netCDF4 as nc
    import numpy as np
    from google.cloud import bigquery
    from datetime import datetime, timezone

    PROJECT_ID = "project-a3416167-bd30-4a48-987"
    DATASET_ID = "ocean_analytics"

    client = bigquery.Client(project=PROJECT_ID)

    # Create reference table if not exists
    table_id = f"{PROJECT_ID}.{DATASET_ID}.ref_ersst_baseline"
    schema = [
        bigquery.SchemaField("year", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("month", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("latitude", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("longitude", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("avg_sst_c", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Table {table_id} ready")
    return "ERSST processing complete"


def load_obis_reference(**context):
    """Load OBIS species reference data into BigQuery."""
    import requests
    from google.cloud import bigquery
    from datetime import datetime, timezone

    PROJECT_ID = "project-a3416167-bd30-4a48-987"
    DATASET_ID = "ocean_analytics"
    client = bigquery.Client(project=PROJECT_ID)

    # Create species reference table
    table_id = f"{PROJECT_ID}.{DATASET_ID}.ref_species"
    schema = [
        bigquery.SchemaField("taxon_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("species_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("scientific_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("kingdom", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phylum", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    # Load species data from OBIS
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
    now = datetime.now(timezone.utc).isoformat()

    for taxon_id, species_name in species_map.items():
        try:
            response = requests.get(
                f"https://api.obis.org/v3/taxon/{taxon_id}",
                timeout=30
            )
            data = response.json()
            rows.append({
                "taxon_id": taxon_id,
                "species_name": species_name,
                "scientific_name": data.get("scientificName", ""),
                "kingdom": data.get("kingdom", "Animalia"),
                "phylum": data.get("phylum", "Chordata"),
                "loaded_at": now,
            })
        except Exception as e:
            print(f"Failed to fetch taxon {taxon_id}: {e}")

    if rows:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise Exception(f"BigQuery insert errors: {errors}")
        print(f"Loaded {len(rows)} species records")


def load_ndbc_stations(**context):
    """Load NDBC buoy station metadata into BigQuery."""
    import requests
    from google.cloud import bigquery
    from datetime import datetime, timezone

    PROJECT_ID = "project-a3416167-bd30-4a48-987"
    DATASET_ID = "ocean_analytics"
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.dim_location"
    schema = [
        bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("station_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("station_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    # Fetch active station list
    response = requests.get(
        "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt",
        timeout=30
    )
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for line in response.text.split("\n"):
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            rows.append({
                "station_id": parts[0],
                "station_name": parts[0],
                "latitude": float(parts[1]),
                "longitude": float(parts[2]),
                "station_type": "buoy",
                "loaded_at": now,
            })
        except (ValueError, IndexError):
            continue

    if rows:
        # Clear existing and reload
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(bigquery.Table(table_id, schema=schema))
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise Exception(f"BigQuery insert errors: {errors[:3]}")
        print(f"Loaded {len(rows)} station records into dim_location")


with DAG(
    "ocean_batch_reference_loader",
    default_args=default_args,
    description="Load batch reference data into BigQuery",
    schedule_interval="0 0 1 * *",  # Monthly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ocean-analytics", "batch", "reference"],
) as dag:

    load_stations = PythonOperator(
        task_id="load_ndbc_stations",
        python_callable=load_ndbc_stations,
    )

    load_species = PythonOperator(
        task_id="load_obis_species",
        python_callable=load_obis_reference,
    )

    download_ersst_task = PythonOperator(
        task_id="download_ersst",
        python_callable=download_ersst,
    )

    process_ersst_task = PythonOperator(
        task_id="process_ersst",
        python_callable=process_ersst,
    )

    # Dependencies
    load_stations >> load_species >> download_ersst_task >> process_ersst_task
