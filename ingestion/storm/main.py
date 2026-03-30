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
TOPIC_ID = os.environ.get("PUBSUB_STORM_TOPIC", "storm-tracks-topic")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))  # 1 hour

# NOAA NHC GeoJSON feeds for active storms
NHC_FEEDS = {
    "atlantic": "https://www.nhc.noaa.gov/CurrentStorms.json",
}

# USGS Earthquake feed as storm proxy when no active storms
# We use recent significant earthquakes to demonstrate the pipeline
USGS_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_active_storms():
    """Fetch active tropical storms from NOAA NHC."""
    storms = []
    try:
        response = requests.get(NHC_FEEDS["atlantic"], timeout=30)
        response.raise_for_status()
        data = response.json()
        active = data.get("activeStorms", [])
        logger.info(f"Found {len(active)} active Atlantic storms")
        for storm in active:
            storms.append({
                "storm_id": storm.get("id", ""),
                "name": storm.get("name", ""),
                "basin": "atlantic",
                "storm_type": storm.get("stormType", ""),
                "latitude": storm.get("latitudeNumeric", None),
                "longitude": storm.get("longitudeNumeric", None),
                "wind_speed_mph": storm.get("intensity", None),
                "movement_speed_mph": storm.get("movementSpd", None),
                "movement_direction": storm.get("movementDir", None),
                "pressure_mb": storm.get("pressure", None),
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source": "noaa_nhc",
                "is_active": True,
            })
    except requests.RequestException as e:
        logger.error(f"Failed to fetch NHC storms: {e}")
    return storms


def fetch_significant_earthquakes():
    """
    Fetch significant seismic events as supplementary ocean events.
    Used when no active storms exist to keep pipeline active.
    """
    events = []
    try:
        response = requests.get(USGS_FEED, timeout=30)
        response.raise_for_status()
        data = response.json()
        features = data.get("features", [])
        logger.info(f"Fetched {len(features)} significant seismic events")
        for f in features[:10]:  # limit to 10
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [None, None, None])
            events.append({
                "storm_id": f"eq_{f.get('id', '')}",
                "name": props.get("place", "Unknown"),
                "basin": "pacific" if coords[0] and coords[0] < -90 else "atlantic",
                "storm_type": "earthquake",
                "latitude": coords[1],
                "longitude": coords[0],
                "magnitude": props.get("mag", None),
                "depth_km": coords[2],
                "wind_speed_mph": None,
                "pressure_mb": None,
                "observed_at": datetime.fromtimestamp(
                    props.get("time", 0) / 1000, tz=timezone.utc
                ).isoformat(),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source": "usgs_earthquake",
                "is_active": True,
            })
    except requests.RequestException as e:
        logger.error(f"Failed to fetch earthquake data: {e}")
    return events


def publish_event(event):
    """Publish a storm/event record to Pub/Sub."""
    message_data = json.dumps(event).encode("utf-8")
    future = publisher.publish(
        topic_path,
        message_data,
        storm_type=event["storm_type"],
        source=event["source"],
        basin=event["basin"],
    )
    return future.result()


def run():
    """Main ingestion loop."""
    logger.info(f"Starting storm/event ingestion — publishing to {topic_path}")
    logger.info(f"Poll interval: {POLL_INTERVAL} seconds")

    while True:
        start_time = time.time()
        published = 0
        failed = 0

        # Try active storms first
        events = fetch_active_storms()

        # If no active storms, use earthquake data to keep pipeline active
        if not events:
            logger.info("No active storms — fetching significant seismic events")
            events = fetch_significant_earthquakes()

        for event in events:
            try:
                if event["latitude"] and event["longitude"]:
                    publish_event(event)
                    logger.info(f"Published: {event['storm_type']} — {event['name']}")
                    published += 1
                else:
                    logger.warning(f"Skipping event {event['storm_id']} — no coordinates")
            except Exception as e:
                logger.error(f"Failed to publish event: {e}")
                failed += 1

        logger.info(f"Cycle complete — published {published}, failed {failed}")

        elapsed = time.time() - start_time
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        logger.info(f"Elapsed {elapsed:.1f}s — sleeping {sleep_time:.0f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    run()
