import logging
from typing import List, Union, Optional

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery
from google.oauth2 import service_account

from src.shared import config_logger

config_logger('info')
logger = logging.getLogger(__name__)

def upload_to_gcs(
        bucket,
        object_name,
        local_file
        ) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    logger.info(f"File {local_file} uploaded to {object_name}")

def blob_exists(
        bucket,
        object_name):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    return blob.exists()

def create_bq_external_table_operator(
        projectID: str,
        bucket: str,
        object_name: str,
        dataset: str,
        table: str,
        format: str
        ) -> BigQueryCreateExternalTableOperator:
    """
    Create an external BigQuery table operator based on a Parquet file stored in GCS.
    Args:
        projectID: The ID of the GCP project where the table will be created.
        bucket: The name of the GCS bucket containing the Parquet file.
        object_name: The path to the Parquet file in GCS.
        dataset: The BigQuery dataset where the table will be created.
        table: The name of the BigQuery table to be created.
        format: The format of the file on GS (e.g., PARQUET, JSON etc)
    Returns: 
        BigQueryCreateExternalTableOperator
    """
    BQoperator = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": projectID,
                "datasetId": dataset,
                "tableId": table,
            },
            "externalDataConfiguration": {
                "sourceFormat": format,
                "sourceUris": [f"gs://{bucket}/{object_name}"],
            },
        },
        )
    return BQoperator

def get_data_from_bq_operator(
        credentials_path: str,
        QUERY: str
    ):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    logger.info(f"Instantiating BQ Client with credentials from {credentials_path}")
    client = bigquery.Client(
        credentials=credentials)
    query_job = client.query(QUERY)  # API request
    data = query_job.result()  # Waits for query to finish
    return data.to_dataframe() # Return as a pandas DataFrame

def create_table(
    client: bigquery.Client,
    full_table_id: str,
    schema: List[bigquery.SchemaField],
    time_partition_field:str,
    clustering_fields: List[str],
    ttl_days: Optional[int] = 35  # time to live in days
) -> None:
    ttl_ms = ttl_days * 24 * 60 * 60 * 1000
    table = bigquery.Table(full_table_id, schema=schema)
    if time_partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            field=time_partition_field,
            type_=bigquery.TimePartitioningType.DAY,
            expiration_ms=ttl_ms)
    if clustering_fields:
        table.clustering_fields = clustering_fields
    table.require_partition_filter = True
    client.create_table(table)
    logger.info(f"Created {full_table_id} clustered on {clustering_fields} and partitioned by {time_partition_field} with TTL: {ttl_days} days")

def bq_load_parquet_append(
    credentials_path: str,    
    gcs_uris: Union[str, List[str]],
    full_table_id: str,
    schema: List[bigquery.SchemaField],
    time_partition_field: str,
    clustering_fields: List[str]
) -> bigquery.LoadJob:
    """
    Load Parquet file(s) from GCS into BigQuery with WRITE_APPEND.
    Pass a wildcard URI or a list of URIs.
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    logger.info(f"Instantiating BQ Client with credentials from {credentials_path}")
    client = bigquery.Client(
        credentials=credentials)

    try:
        client.get_table(full_table_id)
    except NotFound:
        create_table(
            client,
            full_table_id=full_table_id,
            schema=schema,
            time_partition_field=time_partition_field,
            clustering_fields=clustering_fields
        )

    # Build load job config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
        autodetect=False,
    )
    # Submit job
    job = client.load_table_from_uri(
        gcs_uris,                # str or list[str]
        full_table_id,
        job_config=job_config,
    )
    result = job.result()        # wait for completion (raises on error)
    # Basic report
    table = client.get_table(full_table_id)
    print(f"Loaded {result.output_rows} rows into {full_table_id}. Now {table.num_rows} total rows.")
    return job

if __name__ == "__main__":
    credentials_path = "/home/onur/gcp-keys/stocks-455113-eb2c3f563c78.json"
    df = get_data_from_bq_operator(
        credentials_path,
        f"SELECT DISTINCT(symbol) FROM stocks_user_data.ETFS_to_track"
    )
    print(df)
