variable "service_name" {
  description = "service_name"
  type        = string
}

variable "region" {
  description = "region"
  type        = string
}

variable "zone" {
  description = "zone"
  type        = string
}

variable "machine_type" {
  description = "machine_type"
  type        = string
}

variable "boot_disk_image" {
  description = "boot_disk_image"
  type        = string
}

variable "boot_disk_size" {
  description = "boot_disk_size"
  type        = string
}

variable "metadata" {
  description = "metadata"
  type        = map(any)
  default     = {}
}

variable "tags" {
  description = "tags"
  type        = any
}

variable "google_service_account_email" {
  description = "google_service_account_email"
  type        = string
}

variable "metadata_startup_script" {
  description = "metadata_startup_script"
  type        = string
  default     = null
}

variable "boot_disk_type" {
  description = "boot_disk_type"
  type        = string
  default     = "pd-standard"
  validation {
    condition     = contains(["pd-standard", "pd-balanced", "pd-ssd"], var.boot_disk_type)
    error_message = "Such as pd-standard, pd-balanced or pd-ssd"
  }
}

variable "firewall_name" {
  type = string
}

variable "allow" {
  description = "allow"
  type = map(object({
    protocol = string
    ports    = list(string)
  }))
}

variable "source_ranges" {
  description = "e.g., 0.0.0.0/0"
  type        = string
  default     = "0.0.0.0/0"
}

variable "ssh_user_1" {
  description = "ssh user name of the VM"
  type        = string
}

variable "private_key_path_1" {
  description = "first private rsa key path of the host machine"
  type        = string
}

variable "gcp_key_path_src" {
  description = "path to the gcp service account key file on the host machine"
  type        = string
}

variable "gcp_key_path_dest" {
  description = "path to the gcp service account key file on the VM"
  type        = string
}

variable "init_script_path" {
  description = "absolute path to the init script file on the host machine"
  type        = string
}
