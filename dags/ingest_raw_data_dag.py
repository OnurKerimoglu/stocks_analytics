"""
DAG for:
1. Downloading ETF data for a given ETF ticker (input parameter), and price and info data for each ETF holding
2. Uploading etf, price and info (fundamental) data to GCS and creating external tables in BQ
3. Running the dlt pipeline to load and merge etf, price and info data into BQ
4. Removing local data
"""
import os
import logging

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import sqlalchemy

from src.shared import config_logger, reformat_json_to_parquet
from src.download_ticker_data import DownloadTickerData
from src.download_etf_data import DownloadETFData
from src.fetch_symbols import FetchSymbols
from src.gc_functions import upload_to_gcs, blob_exists, create_bq_external_table_operator
from src.load_ticker_data_dlt import LoadTickerData

config_logger('info')
logger = logging.getLogger(__name__)

# Set constants
rootpath0 = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
rootpath = '/opt/airflow/'
logger.info(f'AIRFLOW_HOME: {rootpath0}')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
# BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET", 'stocks_dev')
BIGQUERY_DATASET_EXT = 'stocks_raw_ext'
BIGQUERY_DATASET_DLT = 'stocks_raw'

def get_local_raw_data_path(source, ticker):
    if source == 'price':
        ext = '.parquet'
    elif source == 'info':
        ext = '.json'
    else:
        raise ValueError("source must be 'price' or 'info'")
    fpath = os.path.join(rootpath, 'data', source, ticker+ext)
    return fpath

def fetch_file_paths(dirpath, ext):
    # construct a list of absolute paths based on the 
    # files with the specified extension in the self.datapth folder
    fpaths = []
    for f in os.listdir(dirpath):
        if f.endswith(f'.{ext}'):
            fpaths.append(os.path.join(dirpath, f))
    return fpaths

@dag(
    schedule=None,
    start_date=days_ago(1), 
    catchup=False,
    description="Ingest raw data for a given ETF ticker",
    doc_md = __doc__,
    default_args={
        "owner": "Onur",
        "retries": 3,
        "retry_delay": 5
    },
    params={
        'ETF_symbol': Param(
            'QTOP',
            type='string',
            title='ETF Ticker symbol',
            description='Current options: QTOP (S&P Top 100), OEF (Nasdaq top 30), IVV (S&P 500)',
            enum=[
                'QTOP',
                'OEF',
                'IVV'
            ],

        )
    }
)
def ingest_raw_data_dag():
    
    @task
    def get_etf_data(etf_ticker):
        DownloadETFData(
            etf_ticker
            ).download_etf_tickers()
        return etf_ticker

    @task
    def fetch_symbols(filename):
        fpath = os.path.join(rootpath, 'data', filename)
        symbols = FetchSymbols(file = fpath).symbols
        return symbols

    @task
    def get_ticker_price(ticker: str):
        dtd = DownloadTickerData(
            ticker=ticker,
            period='max')
        dtd.download_prices()
        return ticker
    
    @task
    def get_ticker_info(ticker: str):
        dtd = DownloadTickerData(
            ticker=ticker)
        dtd.download_infos()
        return ticker

    @task
    def reformat_json_to_pq(source:str, ticker:str):
        fpath = get_local_raw_data_path(source, ticker)
        if os.path.exists(fpath):
            logger.info(f"Reformatting {fpath} to parquet")
            reformat_json_to_parquet(fpath)
        else:
            logger.warning(f"{fpath} does not exist")
        return ticker
        
    @task
    def ul_to_gcs(source: str, ticker: str):
        logger.info(f"Storing {ticker} price in GCS")
        fpath = os.path.join(rootpath, 'data', source, ticker+'.parquet')
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
    def create_bq_table(source: str, ticker: str, **context):
        logger.info(f"Creating external talbe in BQ for {ticker}")
        fpath = os.path.join(rootpath, 'data', source, ticker+'.parquet')
        object_name=f"{source}/{os.path.basename(fpath)}"
        if blob_exists(BUCKET, object_name):
            fmt=os.path.basename(fpath).split('.')[1].upper()
            table_name = f'{source}_{ticker}'
            logger.info(f"Creating external table based on {fmt} file gs://{BUCKET}/{object_name}")
            operator = create_bq_external_table_operator(
                projectID=PROJECT_ID,
                bucket=BUCKET,
                object_name=object_name,
                dataset=BIGQUERY_DATASET_EXT,
                table=table_name,
                format=fmt)
            operator.execute(context=context)
            logger.info(f"Created table in BQ: {BIGQUERY_DATASET_EXT}.{table_name}")
        else:
            logger.warning(f"Blob {object_name} does not exist")
        return ticker
        
    @task
    def run_dlt_pl(source: str, full_load: bool):
        logger.info(f"Running the dlt pipeline for {source} data")
        load_ticker = LoadTickerData(
            full_load=full_load,
            dest='bigquery',
            dataset_name=BIGQUERY_DATASET_DLT,
            dev_mode=False,
            log_level='info')
        if source == 'etf':
            load_ticker.run_etf_pipeline()
        if source == 'price':
            load_ticker.run_price_pipeline()
        elif source == 'info':
            load_ticker.run_info_pipeline()
        return 'done'
    
    @task
    def remove_local(source: str):
        fpaths = fetch_file_paths(
            os.path.join(rootpath, 'data', source),
            'parquet')
        logger.info(f"Found {len(fpaths)} to remove")
        for fpath in fpaths:
            # os.remove(fpath)
            logger.info(f"Removed {fpath}")
        return 'done'

    ETF_symbol = '{{ params.ETF_symbol }}'
    logger.info(f'Running the ingest_raw_data_dag for {ETF_symbol}')
    # Control Flow
    # ETF tasks
    etf_ticker = get_etf_data(ETF_symbol)
    @task_group(group_id="tg_etf")
    def tg_etf():
        etf_gcs = ul_to_gcs(source='etf', ticker=etf_ticker)
        etf_bqext = create_bq_table(source='etf', ticker=etf_gcs)
    dlt_pipeline_etf = run_dlt_pl(source='etf', full_load=True)
    remove_local_etf = remove_local(source='etf')
    symbols = fetch_symbols(f'ETF_holdings_{str(ETF_symbol)}.csv')
    # Price tasks
    price_symbol_local = get_ticker_price.expand(ticker=symbols)
    @task_group(group_id="tg_price")
    def tg_price():
        price_symbol_gcs = ul_to_gcs.partial(source='price').expand(ticker=price_symbol_local)
        price_symbol_bqext = create_bq_table.partial(source='price').expand(ticker=price_symbol_gcs)
    dlt_pipeline_price = run_dlt_pl(source='price', full_load=True)
    remove_local_price = remove_local(source='price')
    # Info tasks
    info_symbol_local = get_ticker_info.expand(ticker=symbols)
    info_symbol_local_ref = reformat_json_to_pq.partial(source='info').expand(ticker=info_symbol_local)
    @task_group(group_id="tg_info")
    def tg_info():
        info_symbol_gcs = ul_to_gcs.partial(source='info').expand(ticker=info_symbol_local_ref)
        info_symbol_bqext =create_bq_table.partial(source='info').expand(ticker=info_symbol_gcs)
    dlt_pipeline_info = run_dlt_pl(source='info', full_load=True)
    remove_local_info = remove_local(source='info')

    # etf tasks
    etf_ticker >> tg_etf() >> remove_local_etf
    etf_ticker >> dlt_pipeline_etf >> remove_local_etf
    etf_ticker >> symbols
    # price tasks
    symbols >> price_symbol_local
    price_symbol_local >> tg_price() >> remove_local_price
    price_symbol_local >> dlt_pipeline_price >> remove_local_price
    # info tasks
    symbols >> info_symbol_local >> info_symbol_local_ref
    info_symbol_local_ref >> tg_info() >> remove_local_info
    info_symbol_local_ref >> dlt_pipeline_info >> remove_local_info

dag_instance = ingest_raw_data_dag()
