variable "bq_project" {
  description = "The GCP project for the BigQuery dataset."
  default     = "dtc-de-zoomcamp-485417"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset."
  default     = "demo_dataset"
}

variable "location" {
  description = "The location of the GCS bucket."
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket."
  default     = "terraform-demo-bucket-485417"
}

variable "gcs_storage_class" {
  description = "The class of the GCS bucket."
  default     = "STANDARD"
}