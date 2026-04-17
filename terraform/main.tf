# Tell Terraform we're using Google Cloud
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

# Configure the provider — credentials path comes from terraform.tfvars
provider "google" {
  credentials = file(var.credentials_path)
  project     = var.project_id
  region      = var.region
}

# GCS bucket = your DATA LAKE
# Raw files land here before being loaded into BigQuery
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = upper(var.region)
  force_destroy = true

  lifecycle_rule {
    condition { age = 30 }   # auto-delete raw files after 30 days (cost saving)
    action    { type = "Delete" }
  }
  uniform_bucket_level_access = true
}

# BigQuery dataset = your DATA WAREHOUSE
# Transformed, queryable tables live here
resource "google_bigquery_dataset" "warehouse" {
  dataset_id                 = var.bq_dataset_id
  location                   = var.region
  delete_contents_on_destroy = true
}

# Output what was created so you can see it after terraform apply
output "gcs_bucket_name" { value = google_storage_bucket.data_lake.name }
output "bq_dataset"      { value = google_bigquery_dataset.warehouse.dataset_id }