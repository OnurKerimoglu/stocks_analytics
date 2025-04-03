import os
import logging

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import sqlalchemy

from src.shared import config_logger
from src.download_ticker_data import DownloadTickerData
from src.fetch_symbols import FetchSymbols
from src.gc_functions import upload_to_gcs

config_logger('info')
logger = logging.getLogger(__name__)

# Set constants
rootpath0 = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
rootpath = '/opt/airflow/'
logger.info(f'AIRFLOW_HOME: {rootpath0}')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET", 'trips_data_all')

def get_local_data_path(source, ticker):
    if source == 'price':
        ext = '.parquet'
    elif source == 'info':
        ext = '.json'
    else:
        raise ValueError("source must be 'price' or 'info'")
    fpath = os.path.join(rootpath, 'data', source, ticker+ext)
    return fpath

@dag(schedule=None, start_date=days_ago(1), catchup=False)
def ingest_raw_data_dag():

    @task
    def fetch_symbols(filename):
        fpath = os.path.join(rootpath, 'data', filename)
        symbols = FetchSymbols(file = fpath).symbols
        return symbols

    @task
    def download_ticker_price_max(ticker: str):
        dtd = DownloadTickerData(
            ticker=ticker,
            period='max')
        dtd.download_prices()
        return ticker
    
    @task
    def download_ticker_info(ticker: str):
        dtd = DownloadTickerData(
            ticker=ticker)
        dtd.download_infos()
        return ticker

    @task
    def upload_local_to_gcs(source: str, ticker: str):
        logger.info(f"Storing {ticker} price in GCS")
        fpath = get_local_data_path(source, ticker)
        object_name=f"{source}/{os.path.basename(fpath)}"
        if os.path.exists(fpath):
            logger.info(f"Uploading {fpath} to {object_name}")
            upload_to_gcs(
                bucket = BUCKET,
                object_name=object_name,
                local_file=fpath)
        else:
            logger.warning(f"{fpath} does not exist")
        return ticker

    
    @task
    def upload_price_to_bq(ticker: str):
        logger.info(f"Uploading {ticker} price to BQ")
        return ticker
    
    @task
    def upload_info_to_bq(ticker: str):
        logger.info(f"Uploading {ticker} info to BQ")
        return ticker

    @task
    def check_completion_gcs_bq(ticker_gcs:str, ticker_bq:str):
        if ticker_gcs == ticker_bq:
            logger.info(f"Both gcs and bq jobs completed")
            return ticker_gcs
    
    @task
    def remove_local_data(source: str, ticker: str):
        fpath = get_local_data_path(source, ticker)
        if os.path.exists(fpath):
            # os.remove(fpath)
            logger.info(f"Removed {fpath}")
        else:
            logger.warning(f"{fpath} does not exist")

    with TaskGroup(group_id="tg_ticker_raw") as tg:
        symbols = fetch_symbols('default_stocks.csv')
        with TaskGroup(group_id="subtg_ticker_raw_price") as subtg1:
            price_symbol_local = download_ticker_price_max.expand(ticker=symbols)
            price_symbol_gcs = upload_local_to_gcs.partial(source='price').expand(ticker=price_symbol_local)
            price_symbol_bq = upload_price_to_bq.expand(ticker=price_symbol_local)
            price_symbol = check_completion_gcs_bq.expand(ticker_gcs=price_symbol_gcs, ticker_bq=price_symbol_bq)
            remove_local_data.partial(source='price').expand(ticker=price_symbol)
        with TaskGroup(group_id="subtg_ticker_raw_info") as subtg2:
            info_symbol_local = download_ticker_info.expand(ticker=symbols)
            info_symbol_gcs = upload_local_to_gcs.partial(source='info').expand(ticker=info_symbol_local)
            info_symbol_bq = upload_info_to_bq.expand(ticker=info_symbol_local)
            info_symbol = check_completion_gcs_bq.expand(ticker_gcs=info_symbol_gcs, ticker_bq=info_symbol_bq)
            remove_local_data.partial(source='info').expand(ticker=info_symbol)

dag_instance = ingest_raw_data_dag()
