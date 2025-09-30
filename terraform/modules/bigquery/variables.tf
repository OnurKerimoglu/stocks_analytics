variable "project"  { type = string }
# variable "region"    { type = string }

variable "region" {
  type    = string
  default = "europe-west3"
}

variable "src_dataset_raw" {
  type    = string
  default = "stocks_raw"         # prod dataset (module default)
}

variable "dst_dataset_raw" {
  type    = string
  default = "stocks_raw_test"    # test dataset to be cloned from prod (module default)
}

variable "transfer_schedule" {
  type    = string
  default = "every sunday 23:00"
}
