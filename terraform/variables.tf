# Declaring variables here keeps main.tf clean and generic.
# Anyone can reuse your infrastructure code with their own values.
variable "credentials_path" {
  description = "Path to GCP service account JSON key file"
  type        = string
}
variable "project_id" {
  description = "Your GCP Project ID"
  type        = string
}
variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}
variable "bucket_name" {
  description = "GCS bucket name — must be globally unique across all of GCP"
  type        = string
}
variable "bq_dataset_id" {
  description = "BigQuery dataset name"
  type        = string
  default     = "frontend_jobs"
}