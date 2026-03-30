output "gcs_bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.ocean_analytics.dataset_id
}

output "pubsub_buoy_topic" {
  value = google_pubsub_topic.buoy_readings.name
}

output "pubsub_whale_topic" {
  value = google_pubsub_topic.whale_sightings.name
}

output "pubsub_storm_topic" {
  value = google_pubsub_topic.storm_tracks.name
}
