terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ── GCS BUCKET ──────────────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-ocean-analytics-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project     = "ocean-analytics"
    environment = "dev"
  }
}

# GCS folders (bronze/silver/gold)
resource "google_storage_bucket_object" "bronze_buoy" {
  name    = "bronze/buoy-readings/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "bronze_whale" {
  name    = "bronze/whale-sightings/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "bronze_storm" {
  name    = "bronze/storm-tracks/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "silver_buoy" {
  name    = "silver/buoy-readings/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "silver_whale" {
  name    = "silver/whale-sightings/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "silver_storm" {
  name    = "silver/storm-tracks/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "gold" {
  name    = "gold/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "dataflow_staging" {
  name    = "dataflow/staging/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "dataflow_temp" {
  name    = "dataflow/temp/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

# ── BIGQUERY ─────────────────────────────────────────────────
resource "google_bigquery_dataset" "ocean_analytics" {
  dataset_id    = var.bigquery_dataset
  friendly_name = "Ocean Analytics"
  description   = "Oceanographic data pipeline — buoy readings, whale sightings, storm tracks"
  location      = var.region

  labels = {
    project     = "ocean-analytics"
    environment = "dev"
  }
}

# ── PUB/SUB TOPICS ───────────────────────────────────────────
resource "google_pubsub_topic" "buoy_readings" {
  name                       = "buoy-readings-topic"
  message_retention_duration = "604800s"

  labels = {
    project = "ocean-analytics"
  }
}

resource "google_pubsub_topic" "whale_sightings" {
  name                       = "whale-sightings-topic"
  message_retention_duration = "604800s"

  labels = {
    project = "ocean-analytics"
  }
}

resource "google_pubsub_topic" "storm_tracks" {
  name                       = "storm-tracks-topic"
  message_retention_duration = "604800s"

  labels = {
    project = "ocean-analytics"
  }
}

# ── PUB/SUB SUBSCRIPTIONS ────────────────────────────────────
resource "google_pubsub_subscription" "buoy_readings_sub" {
  name  = "buoy-readings-subscription"
  topic = google_pubsub_topic.buoy_readings.name

  ack_deadline_seconds = 60

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    project = "ocean-analytics"
  }
}

resource "google_pubsub_subscription" "whale_sightings_sub" {
  name  = "whale-sightings-subscription"
  topic = google_pubsub_topic.whale_sightings.name

  ack_deadline_seconds = 60

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    project = "ocean-analytics"
  }
}

resource "google_pubsub_subscription" "storm_tracks_sub" {
  name  = "storm-tracks-subscription"
  topic = google_pubsub_topic.storm_tracks.name

  ack_deadline_seconds = 60

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    project = "ocean-analytics"
  }
}

# ── DEAD LETTER TOPIC ────────────────────────────────────────
resource "google_pubsub_topic" "dead_letter" {
  name = "ocean-dead-letter-topic"

  labels = {
    project = "ocean-analytics"
  }
}
