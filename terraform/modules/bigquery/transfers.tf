locals {
  refresh_clone_raw_query = templatefile(
    "${path.module}/sql/queries/refresh_test_raw.sql.tpl",
    {
      src_project = var.project
      src_dataset = var.src_dataset_raw
      dst_project = var.project
      dst_dataset = var.dst_dataset_raw
      location    = var.region
    }
  )
}

resource "google_bigquery_data_transfer_config" "weekly_refresh_clone_raw" {
  project        = var.project
  display_name   = "Weekly: Clone prod -> dev"
  data_source_id = "scheduled_query"
  location       = var.region
  schedule       = var.transfer_schedule

  params = {
    query          = local.refresh_clone_raw_query
  }
}
