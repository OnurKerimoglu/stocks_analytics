"""
DAG for:
1. Downloading ETF data for a given list of ETF tickers (input parameter)
2. Downloading price and info data for each ticker held by ETFs
3. Uploading etf, price and info (fundamental) data to GCS and creating external tables in BQ
4. Running the dlt pipeline to load and merge etf, price and info data into BQ
5. Removing local data
"""
import ast
import os
import logging

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

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

def fetch_symbols_for_etf(filename):
    fpath = os.path.join(rootpath, 'data', filename)
    symbols = FetchSymbols(file = fpath).symbols
    return symbols
    
@dag(
    schedule='0 1 * * *',
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
        'ETF_symbols': Param(
            ["QTOP", "OEF"],
            type="array",
            title='List of ETF Ticker symbols',
            examples=["QTOP", "OEF"]
            # description="E.g., ["QTOP"] for iShares Nasdaq top 30",
        )
    }
)
def ingest_raw_data_dag():
    @task
    def get_etf_symbols_from_params(**context):
        etf_symbols = context["params"]["ETF_symbols"]
        return etf_symbols  

    @task
    def get_etf_data(ETF_symbol: str):
        DownloadETFData(
            [ETF_symbol]
            ).download_etf_tickers()
        return ETF_symbol

    @task
    def fetch_unique_symbols_for_etfs(ETF_symbols: list):
        # collect all symbols for each ETF
        logger.info(f'Getting ticker symbols for {len(ETF_symbols)} ETFs:')
        symbols_all = []
        for ETF_symbol in ETF_symbols:
            logger.info(f'Getting for {ETF_symbol}')
            symbols_etf = fetch_symbols_for_etf(f'ETF_holdings_{str(ETF_symbol)}.csv')
            # add the ETF_symbol itself to the list
            logger.info(f'Adding {ETF_symbol} to the list of symbols')
            symbols_etf += [ETF_symbol]
            symbols_all +=  symbols_etf
            logger.info(f'Number of all symbols: {len(symbols_all)}')
        # eliminate duplicates
        symbols = list(set(symbols_all))
        logger.info(f'Returnig {len(symbols)} unique symbols')
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
        logger.info(f"Storing {ticker} data in GCS")
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
            os.remove(fpath)
            logger.info(f"Removed {fpath}")
        return 'done'
    
    triggered_ticker_transformations_dag = TriggerDagRunOperator(
        trigger_dag_id="ticker_transformations_dag",
        task_id="triggered_ticker_transf_dag",
        wait_for_completion=False,
        deferrable=False
    )
    
    # ETF_symbols = '{{ params.ETF_symbols }}'
    # Parse the ETF_symbols to be processed from the input params
    ETF_symbols = get_etf_symbols_from_params()
    logger.info(f'Running the ingest_raw_data_dag for {ETF_symbols}')
    
    # Control Flow
    # ETF tasks
    ETF_symbol_local = get_etf_data.expand(ETF_symbol=ETF_symbols)

    symbols = fetch_unique_symbols_for_etfs(ETF_symbols)

    @task_group(group_id="tg_etf")
    def tg_etf():
        ETF_symbol_gcs = ul_to_gcs.partial(source='etf').expand(ticker=ETF_symbol_local)
        etf_bqext = create_bq_table.partial(source='etf').expand(ticker=ETF_symbol_gcs)
    dlt_pipeline_etf = run_dlt_pl(source='etf', full_load=True)
    remove_local_etf = remove_local(source='etf')
    
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
    ETF_symbol_local >> symbols >> remove_local_etf
    ETF_symbol_local >> tg_etf() >> remove_local_etf
    ETF_symbol_local  >> dlt_pipeline_etf >> remove_local_etf

    # price tasks
    symbols >> price_symbol_local
    price_symbol_local >> tg_price() >> remove_local_price
    price_symbol_local >> dlt_pipeline_price >> remove_local_price

    # info tasks
    symbols >> info_symbol_local >> info_symbol_local_ref
    info_symbol_local_ref >> tg_info() >> remove_local_info
    info_symbol_local_ref >> dlt_pipeline_info >> remove_local_info

    # trigger ticker_transformations_dag
    dlt_pipeline_info >> triggered_ticker_transformations_dag 
    dlt_pipeline_price >> triggered_ticker_transformations_dag

dag_instance = ingest_raw_data_dag()
