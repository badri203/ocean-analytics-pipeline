variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-east1"
}

variable "credentials_file" {
  description = "Path to GCP service account key"
  type        = string
}

variable "bigquery_dataset" {
  description = "BigQuery dataset name"
  type        = string
  default     = "ocean_analytics"
}
