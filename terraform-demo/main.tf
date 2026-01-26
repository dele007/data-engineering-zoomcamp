terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.bq_project
  region  = "us-central1"
}




resource "google_storage_bucket" "terraform-bucket-demo" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}




resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = var.bq_dataset_name
  location = var.location
}