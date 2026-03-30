import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-a3416167-bd30-4a48-987")
TOPIC_ID = os.environ.get("PUBSUB_BUOY_TOPIC", "buoy-readings-topic")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))  # 1 hour

NDBC_STATIONS_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_all_buoy_readings():
    """Fetch latest readings from all active NDBC buoy stations."""
    logger.info("Fetching buoy readings from NOAA NDBC...")
    try:
        response = requests.get(NDBC_STATIONS_URL, timeout=30)
        response.raise_for_status()
        return parse_buoy_data(response.text)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch buoy data: {e}")
        return []


def parse_buoy_data(raw_text):
    """Parse the NDBC latest observations text file."""
    readings = []
    lines = raw_text.strip().split("\n")

    # Skip header lines (start with #)
    data_lines = [l for l in lines if not l.startswith("#") and l.strip()]

    for line in data_lines:
        parts = line.split()
        if len(parts) < 19:
            continue

        try:
            reading = {
                "station_id": parts[0],
                "latitude": float(parts[1]),
                "longitude": float(parts[2]),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "wind_direction_deg": None if parts[3] == "MM" else float(parts[3]),
                "wind_speed_ms": None if parts[4] == "MM" else float(parts[4]),
                "wind_gust_ms": None if parts[5] == "MM" else float(parts[5]),
                "wave_height_m": None if parts[6] == "MM" else float(parts[6]),
                "dominant_wave_period_s": None if parts[7] == "MM" else float(parts[7]),
                "avg_wave_period_s": None if parts[8] == "MM" else float(parts[8]),
                "wave_direction_deg": None if parts[9] == "MM" else float(parts[9]),
                "sea_level_pressure_hpa": None if parts[10] == "MM" else float(parts[10]),
                "pressure_tendency_hpa": None if parts[11] == "MM" else float(parts[11]),
                "air_temp_c": None if parts[12] == "MM" else float(parts[12]),
                "water_temp_c": None if parts[13] == "MM" else float(parts[13]),
                "dewpoint_temp_c": None if parts[14] == "MM" else float(parts[14]),
                "visibility_nmi": None if parts[15] == "MM" else float(parts[15]),
                "tide_ft": None if parts[16] == "MM" else float(parts[16]),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source": "noaa_ndbc",
            }
            readings.append(reading)
        except (ValueError, IndexError) as e:
            logger.warning(f"Skipping malformed line for station {parts[0]}: {e}")
            continue

    logger.info(f"Parsed {len(readings)} buoy readings")
    return readings


def publish_reading(reading):
    """Publish a single buoy reading to Pub/Sub."""
    message_data = json.dumps(reading).encode("utf-8")
    future = publisher.publish(
        topic_path,
        message_data,
        station_id=reading["station_id"],
        source=reading["source"],
    )
    return future.result()


def run():
    """Main ingestion loop."""
    logger.info(f"Starting buoy ingestion — publishing to {topic_path}")
    logger.info(f"Poll interval: {POLL_INTERVAL} seconds")

    while True:
        start_time = time.time()
        readings = fetch_all_buoy_readings()

        if readings:
            published = 0
            failed = 0
            for reading in readings:
                try:
                    publish_reading(reading)
                    published += 1
                except Exception as e:
                    logger.error(f"Failed to publish reading for station {reading['station_id']}: {e}")
                    failed += 1

            logger.info(f"Published {published} readings, {failed} failed")
        else:
            logger.warning("No readings fetched this cycle")

        elapsed = time.time() - start_time
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        logger.info(f"Cycle complete in {elapsed:.1f}s — sleeping {sleep_time:.0f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    run()
