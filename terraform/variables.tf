variable "project" {
  description = "Project"
  default     = "stocks-455113"
}

variable "zone" {
  description = "Zone"
  #Update the below to your desired region
  default     = "europe-west1-b"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "stocks_dev"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "stocks-455113-raw"
}

variable "gcs_dev_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "stocks-455113-raw-dev"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gce_vm1_service_name" {
  description = "Compute Engine VM1 Name"
  default     = "stocks-scheduler-1"
}

variable "gce_vm1_machine_type" {
  description = "Compute Engine VM1 Machine Type"
  default     = "n2d-standard-4"
}

variable "gce_vm1_boot_disk_image" {
  description = "Compute Engine VM1 Boot Image (OS)"
  default     = "ubuntu-minimal-2204-jammy-v20250414"
}

variable "gce_vm1_boot_disk_size" {
  description = "Compute Engine VM1 Boot Size"
  default     = 30
}

variable "gce_vm2_service_name" {
  description = "Compute Engine VM2 Name"
  default     = "stocks-scheduler-2"
}

variable "gce_vm2_machine_type" {
  description = "Compute Engine VM2 Machine Type"
  default     = "n2d-standard-4"
}

variable "gce_vm2_boot_disk_image" {
  description = "Compute Engine VM2 Boot Image (OS)"
  default     = "ubuntu-minimal-2204-jammy-v20250414"
}

variable "gce_vm2_boot_disk_size" {
  description = "Compute Engine VM2 Boot Size"
  default     = 30
}

variable "ssh_user_1" {sensitive=true}
variable "ssh_user_2" {sensitive=true}

variable "public_key_path_1" {}
variable "public_key_path_2" {}

variable "private_key_path_1" {}

variable "repo_name" {
  description = "The Artifact Registry repository name"
  default     = "stocks-analytics-repo"
  type        = string
}

variable "gcp_key_path_src" {sensitive=true}
variable "gcp_key_path_dest" {sensitive=true}
