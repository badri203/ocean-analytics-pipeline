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
TOPIC_ID = os.environ.get("PUBSUB_WHALE_TOPIC", "whale-sightings-topic")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "1800"))  # 30 minutes

OBIS_API_URL = "https://api.obis.org/v3/occurrence"

# OBIS taxon IDs for marine mammals
SPECIES_MAP = {
    137092: "orca",
    137091: "humpback whale",
    137090: "gray whale",
    137119: "minke whale",
    254972: "bottlenose dolphin",
    137098: "sperm whale",
    137094: "blue whale",
    137096: "fin whale",
}

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_species_occurrences(taxon_id, species_name):
    """Fetch recent occurrences for a species from OBIS."""
    try:
        params = {
            "taxonid": taxon_id,
            "size": 20,
            "after": "2020-01-01",
        }
        response = requests.get(OBIS_API_URL, params=params, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])
        logger.info(f"Fetched {len(results)} occurrences for {species_name}")
        return results
    except requests.RequestException as e:
        logger.error(f"Failed to fetch occurrences for {species_name}: {e}")
        return []


def parse_occurrence(raw, species_name):
    """Normalize an OBIS occurrence record into our schema."""
    return {
        "sighting_id": str(raw.get("id", "")),
        "species": species_name,
        "scientific_name": raw.get("scientificName", ""),
        "latitude": raw.get("decimalLatitude", None),
        "longitude": raw.get("decimalLongitude", None),
        "count": raw.get("individualCount", 1) or 1,
        "depth_m": raw.get("depth", None),
        "sighted_at": raw.get("eventDate", datetime.now(timezone.utc).isoformat()),
        "dataset": raw.get("datasetName", "OBIS"),
        "country": raw.get("country", None),
        "locality": raw.get("locality", None),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "obis",
        "is_simulated": False,
    }


def publish_sighting(sighting):
    """Publish a sighting to Pub/Sub."""
    message_data = json.dumps(sighting).encode("utf-8")
    future = publisher.publish(
        topic_path,
        message_data,
        species=sighting["species"],
        source=sighting["source"],
    )
    return future.result()


def run():
    """Main ingestion loop."""
    logger.info(f"Starting whale sighting ingestion (OBIS) — publishing to {topic_path}")
    logger.info(f"Tracking {len(SPECIES_MAP)} species | Poll interval: {POLL_INTERVAL}s")

    while True:
        start_time = time.time()
        published = 0
        failed = 0

        for taxon_id, species_name in SPECIES_MAP.items():
            occurrences = fetch_species_occurrences(taxon_id, species_name)

            for raw in occurrences:
                try:
                    sighting = parse_occurrence(raw, species_name)
                    if sighting["latitude"] and sighting["longitude"]:
                        publish_sighting(sighting)
                        published += 1
                    else:
                        logger.warning(f"Skipping {species_name} record — no coordinates")
                except Exception as e:
                    logger.error(f"Failed to publish {species_name} sighting: {e}")
                    failed += 1

        logger.info(f"Cycle complete — published {published}, failed {failed}")

        elapsed = time.time() - start_time
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        logger.info(f"Elapsed {elapsed:.1f}s — sleeping {sleep_time:.0f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    run()
