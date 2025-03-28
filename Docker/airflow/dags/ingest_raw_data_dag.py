import os
import logging

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import sqlalchemy

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

PG_HOST = os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")


# csv_file = "yellow_tripdata_2021-01.csv.gz"
# parquet_file = dataset_file.replace('.csv.gz', '.parquet')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET", 'trips_data_all')


@dag(schedule=None, start_date=days_ago(1), catchup=False)
def ingest_raw_data_dag():
    
    @task
    def fetch_tickers():
        TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN"]
        return TICKERS

    @task
    def fetch_ticker_data(ticker: str):
        print(f"Fetching data for {ticker}")
        return ticker  # Passing ticker to next step

    @task
    def convert_to_parquet(ticker: str):
        print(f"Processing data for {ticker}")
        return ticker

    @task
    def upload_to_gcs(ticker: str):
        print(f"Storing {ticker} in GCS")
        return ticker
    
    @task
    def upload_to_bq(ticker: str):
        print(f"Creating bq table for {ticker}")

    with TaskGroup(group_id="ticker_processing") as tg:
        tickers = fetch_tickers()
        fetched = fetch_ticker_data.expand(ticker=tickers)
        converted = convert_to_parquet.expand(ticker=fetched)
        uploaded = upload_to_gcs.expand(ticker=converted)
        upload_to_bq.expand(ticker=uploaded)

dag_instance = ingest_raw_data_dag()
