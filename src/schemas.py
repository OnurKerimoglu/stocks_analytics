from google.cloud import bigquery


table_schema_forecast_raw = [
    bigquery.SchemaField("Date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Close", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Returns", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Ticker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("asof", "TIMESTAMP", mode="REQUIRED"),
]