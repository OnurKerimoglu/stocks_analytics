from google.cloud import bigquery


table_schema_forecast_raw = [
    bigquery.SchemaField("Date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Close", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Returns", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Ticker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("asof", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("model_registry_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("model_alias", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("model_version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("model_trained_at", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("model_run_id", "STRING", mode="NULLABLE"),
]