import logging

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage, bigquery

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
        PROJECT_ID: str,
        QUERY: str
    ):
    client = bigquery.Client(project=PROJECT_ID)

    query_job = client.query(QUERY)  # API request
    data = query_job.result()  # Waits for query to finish
    return data.to_dataframe() # Return as a pandas DataFrame
