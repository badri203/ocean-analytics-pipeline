import argparse
import json
import logging
import uuid
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp import pubsub

PROJECT_ID = "project-a3416167-bd30-4a48-987"
DATASET_ID = "ocean_analytics"
BUCKET = f"{PROJECT_ID}-ocean-analytics-lake"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── PARSE FUNCTIONS ──────────────────────────────────────────

class ParseBuoyMessage(beam.DoFn):
    """Parse and validate buoy readings from Pub/Sub."""

    def process(self, message):
        try:
            data = json.loads(message.decode("utf-8"))

            # Validate required fields
            if not data.get("station_id"):
                return

            now = datetime.now(timezone.utc).isoformat()
            yield {
                "reading_id": str(uuid.uuid4()),
                "station_id": data.get("station_id", ""),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "timestamp": data.get("timestamp", now),
                "wind_direction_deg": data.get("wind_direction_deg"),
                "wind_speed_ms": data.get("wind_speed_ms"),
                "wind_gust_ms": data.get("wind_gust_ms"),
                "wave_height_m": data.get("wave_height_m"),
                "dominant_wave_period_s": data.get("dominant_wave_period_s"),
                "avg_wave_period_s": data.get("avg_wave_period_s"),
                "wave_direction_deg": data.get("wave_direction_deg"),
                "sea_level_pressure_hpa": data.get("sea_level_pressure_hpa"),
                "air_temp_c": data.get("air_temp_c"),
                "water_temp_c": data.get("water_temp_c"),
                "dewpoint_temp_c": data.get("dewpoint_temp_c"),
                "visibility_nmi": data.get("visibility_nmi"),
                "tide_ft": data.get("tide_ft"),
                "ingested_at": now,
                "source": data.get("source", "noaa_ndbc"),
                "date": datetime.now(timezone.utc).date().isoformat(),
            }
        except Exception as e:
            logger.error(f"Failed to parse buoy message: {e}")


class ParseWhaleMessage(beam.DoFn):
    """Parse and validate whale sighting messages from Pub/Sub."""

    def process(self, message):
        try:
            import re
            data = json.loads(message.decode("utf-8"))

            if not data.get("species"):
                return
            if not data.get("latitude") or not data.get("longitude"):
                return

            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()

            # Robust date parsing for messy OBIS dates
            raw_date = data.get("sighted_at", now_iso)
            try:
                # Handle date ranges like 2000-09-21/2000-09-21
                if "/" in str(raw_date):
                    raw_date = str(raw_date).split("/")[0]
                # Handle date-only strings
                if re.match(r"^\d{4}-\d{2}-\d{2}$", str(raw_date)):
                    raw_date = raw_date + "T00:00:00+00:00"
                # Handle partial timestamps like 2012-09-14T10:39
                if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$", str(raw_date)):
                    raw_date = raw_date + ":00+00:00"
            except:
                raw_date = now_iso

            yield {
                "sighting_id": data.get("sighting_id", str(uuid.uuid4())),
                "species": data.get("species", "unknown"),
                "scientific_name": data.get("scientific_name", ""),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "count": data.get("count", 1),
                "depth_m": data.get("depth_m"),
                "sighted_at": raw_date,
                "dataset": data.get("dataset", "OBIS"),
                "country": data.get("country"),
                "locality": data.get("locality"),
                "ingested_at": now_iso,
                "source": data.get("source", "obis"),
                "date": now.date().isoformat(),
            }
        except Exception as e:
            logger.error(f"Failed to parse whale message: {e}")


class ParseStormMessage(beam.DoFn):
    """Parse and validate storm/event messages from Pub/Sub."""

    def process(self, message):
        try:
            data = json.loads(message.decode("utf-8"))

            if not data.get("storm_type"):
                return

            now = datetime.now(timezone.utc).isoformat()
            yield {
                "storm_id": data.get("storm_id", str(uuid.uuid4())),
                "name": data.get("name", ""),
                "basin": data.get("basin", ""),
                "storm_type": data.get("storm_type", ""),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "wind_speed_mph": data.get("wind_speed_mph"),
                "pressure_mb": data.get("pressure_mb"),
                "magnitude": data.get("magnitude"),
                "depth_km": data.get("depth_km"),
                "observed_at": data.get("observed_at", now),
                "ingested_at": now,
                "source": data.get("source", ""),
                "date": datetime.now(timezone.utc).date().isoformat(),
            }
        except Exception as e:
            logger.error(f"Failed to parse storm message: {e}")


class WriteToGCS(beam.DoFn):
    """Write raw messages to GCS Bronze layer."""

    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

    def process(self, message):
        from google.cloud import storage
        import uuid

        client = storage.Client()
        bucket = client.bucket(self.bucket)

        now = datetime.now(timezone.utc)
        blob_name = (
            f"bronze/{self.prefix}/"
            f"{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{now.hour:02d}/{uuid.uuid4()}.json"
        )

        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            message.decode("utf-8"),
            content_type="application/json"
        )
        yield message


# ── BIGQUERY SCHEMAS ─────────────────────────────────────────

BUOY_SCHEMA = {
    "fields": [
        {"name": "reading_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "station_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "wind_direction_deg", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "wind_speed_ms", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "wind_gust_ms", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "wave_height_m", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "dominant_wave_period_s", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "avg_wave_period_s", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "wave_direction_deg", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "sea_level_pressure_hpa", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "air_temp_c", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "water_temp_c", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "dewpoint_temp_c", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "visibility_nmi", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "tide_ft", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "source", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    ]
}

WHALE_SCHEMA = {
    "fields": [
        {"name": "sighting_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "species", "type": "STRING", "mode": "REQUIRED"},
        {"name": "scientific_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "depth_m", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "sighted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "dataset", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "locality", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "source", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    ]
}

STORM_SCHEMA = {
    "fields": [
        {"name": "storm_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "basin", "type": "STRING", "mode": "NULLABLE"},
        {"name": "storm_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "wind_speed_mph", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "pressure_mb", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "magnitude", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "depth_km", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "observed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "source", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    ]
}


# ── PIPELINE ─────────────────────────────────────────────────

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--region", default="us-east1")
    parser.add_argument("--runner", default="DirectRunner")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:

        # ── BUOY PIPELINE ──
        buoy_messages = (
            p
            | "Read buoy Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=f"projects/{PROJECT_ID}/subscriptions/buoy-readings-subscription"
            )
        )

        # Write raw to GCS Bronze
        buoy_bronze = (
            buoy_messages
            | "Write buoy to GCS Bronze" >> beam.ParDo(
                WriteToGCS(BUCKET, "buoy-readings")
            )
        )

        # Parse and write to BigQuery Silver
        (
            buoy_bronze
            | "Parse buoy messages" >> beam.ParDo(ParseBuoyMessage())
            | "Write buoy to BigQuery" >> WriteToBigQuery(
                f"{PROJECT_ID}:{DATASET_ID}.fact_buoy_readings",
                schema=BUOY_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # ── WHALE PIPELINE ──
        whale_messages = (
            p
            | "Read whale Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=f"projects/{PROJECT_ID}/subscriptions/whale-sightings-subscription"
            )
        )

        whale_bronze = (
            whale_messages
            | "Write whale to GCS Bronze" >> beam.ParDo(
                WriteToGCS(BUCKET, "whale-sightings")
            )
        )

        (
            whale_bronze
            | "Parse whale messages" >> beam.ParDo(ParseWhaleMessage())
            | "Write whale to BigQuery" >> WriteToBigQuery(
                f"{PROJECT_ID}:{DATASET_ID}.fact_whale_sightings",
                schema=WHALE_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # ── STORM PIPELINE ──
        storm_messages = (
            p
            | "Read storm Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=f"projects/{PROJECT_ID}/subscriptions/storm-tracks-subscription"
            )
        )

        storm_bronze = (
            storm_messages
            | "Write storm to GCS Bronze" >> beam.ParDo(
                WriteToGCS(BUCKET, "storm-tracks")
            )
        )

        (
            storm_bronze
            | "Parse storm messages" >> beam.ParDo(ParseStormMessage())
            | "Write storm to BigQuery" >> WriteToBigQuery(
                f"{PROJECT_ID}:{DATASET_ID}.fact_storm_tracks",
                schema=STORM_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
