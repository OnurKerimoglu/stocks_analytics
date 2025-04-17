terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "stocks-bucket" {
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


# resource "google_bigquery_dataset" "stocks_dataset" {
#   dataset_id = var.bq_dataset_name
#   location   = var.location
# }


module "gcp_cloud_platform" {
  source       = "./modules/google_cloud_platform"
  project_id   = var.project
  account_id   = "compute-engine-account"
  display_name = "Service Account for Compute Engine"
}

# module "gcp_compute_engine_vm1" {
#   source       = "./modules/google_compute_engine"
#   service_name = var.gce_vm1_service_name

#   region       = var.region
#   zone         = var.zone
#   machine_type = var.gce_vm1_machine_type
#   boot_disk_image = var.gce_vm1_boot_disk_image
#   boot_disk_size = var.gce_vm1_boot_disk_size

#   google_service_account_email = module.gcp_cloud_platform.google_service_account_email
#   firewall_name                = "airflow-rule"
#   tags                         = ["http-server", "https-server", "airflow-rule"]
#   allow = {
#     1 = {
#       protocol = "icmp"
#       ports    = null
#     },
#     2 = {
#       protocol = "tcp"
#       ports    = ["22", "8080"]
#     },
#   }
# }


module "gcp_compute_engine_vm2" {
  source       = "./modules/google_compute_engine"
  service_name = var.gce_vm2_service_name

  region       = var.region
  zone         = var.zone
  machine_type = var.gce_vm2_machine_type
  boot_disk_image = var.gce_vm2_boot_disk_image
  boot_disk_size = var.gce_vm2_boot_disk_size

  google_service_account_email = module.gcp_cloud_platform.google_service_account_email
  firewall_name                = "airflow-rule"
  tags                         = ["http-server", "https-server", "airflow-rule"]
  allow = {
    1 = {
      protocol = "icmp"
      ports    = null
    },
    2 = {
      protocol = "tcp"
      ports    = ["22", "8080"]
    },
  }
}