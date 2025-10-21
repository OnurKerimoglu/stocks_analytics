locals {
  refresh_clone_raw_query = templatefile(
    "${path.module}/sql/queries/refresh_clone.sql.tpl",
    {
      src_project = var.project
      src_dataset = var.src_dataset_raw
      dst_project = var.project
      dst_dataset = var.dst_dataset_raw_clone
      location    = var.region
    }
  )
  refresh_clone_refined_query = templatefile(
    "${path.module}/sql/queries/refresh_clone.sql.tpl",
    {
      src_project = var.project
      src_dataset = var.src_dataset_refined
      dst_project = var.project
      dst_dataset = var.dst_dataset_refined_clone
      location    = var.region
    }
  )
  refresh_clone_userdata_query = templatefile(
    "${path.module}/sql/queries/refresh_clone.sql.tpl",
    {
      src_project = var.project
      src_dataset = var.src_dataset_userdata
      dst_project = var.project
      dst_dataset = var.dst_dataset_userdata_clone
      location    = var.region
    }
  )
}

resource "google_bigquery_data_transfer_config" "weekly_refresh_clone_raw" {
  project        = var.project
  display_name   = "Weekly: Clone raw prod"
  data_source_id = "scheduled_query"
  location       = var.region
  schedule       = var.transfer_schedule

  params = {
    query          = local.refresh_clone_raw_query
  }
}

resource "google_bigquery_data_transfer_config" "weekly_refresh_clone_refined" {
  project        = var.project
  display_name   = "Weekly: Clone refined prod"
  data_source_id = "scheduled_query"
  location       = var.region
  schedule       = var.transfer_schedule

  params = {
    query          = local.refresh_clone_refined_query
  }
}

resource "google_bigquery_data_transfer_config" "weekly_refresh_clone_userdata" {
  project        = var.project
  display_name   = "Weekly: Clone userdata prod"
  data_source_id = "scheduled_query"
  location       = var.region
  schedule       = var.transfer_schedule

  params = {
    query          = local.refresh_clone_userdata_query
  }
}
