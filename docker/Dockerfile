FROM python:3.12-slim

WORKDIR /app

RUN pip install uv --no-cache-dir

COPY ingestion/ ./ingestion/

RUN cd ingestion/buoy && uv sync
RUN cd ingestion/whale && uv sync  
RUN cd ingestion/storm && uv sync

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-key.json
ENV GCP_PROJECT_ID=project-a3416167-bd30-4a48-987
ENV PUBSUB_BUOY_TOPIC=buoy-readings-topic
ENV PUBSUB_WHALE_TOPIC=whale-sightings-topic
ENV PUBSUB_STORM_TOPIC=storm-tracks-topic

CMD ["echo", "Run: docker run -v ~/.gcp:/app/.gcp ocean-analytics python ingestion/buoy/main.py"]
