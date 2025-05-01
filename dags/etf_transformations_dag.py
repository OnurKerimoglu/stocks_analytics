import os
import subprocess
import logging

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from src.shared import config_logger
from src.gc_functions import get_data_from_bq_operator

config_logger('info')
logger = logging.getLogger(__name__)

rootpath = os.environ.get("AIRFLOW_HOME")
dbt_dir = os.path.join(rootpath, 'dbt', 'stocks_dbt')

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = 'stocks_raw'
BQ_ETFS_TABLE = 'etfs'


@dag(
    schedule=None,
    start_date=days_ago(1), 
    catchup=False,
    default_args={
        "owner": "Onur",
        "retries": 1,
        "retry_delay": 5
    }
)
def etf_transformations_dag():

    @task
    def fetch_unique_etfs():
        logger.info(f'Fetching unique ETF symbols from {BQ_DATASET}.{BQ_ETFS_TABLE} table')
        df = get_data_from_bq_operator(
            PROJECT_ID,
            f"SELECT DISTINCT(fund_ticker) FROM {BQ_DATASET}.{BQ_ETFS_TABLE}"
        )
        symbols = list(df['fund_ticker'])
        logger.info(f'Returnig {len(symbols)} unique symbols: {symbols}')
        return symbols

    @task
    def etf_tickers_combine(ETF_symbol: str):
        logger.info(f'etf_tickers_combine: {ETF_symbol}')
        vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
        bash_command=f"dbt run -s etf_tickers_combine --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
        result = subprocess.run(
            bash_command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return ETF_symbol

    @task
    def etf_top_ticker_prices(ETF_symbol: str):
        logger.info(f'etf_top_ticker_prices: {ETF_symbol}')
        vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
        # 'etf_top_ticker_prices',     
        bash_command=f"dbt run -s etf_top_ticker_prices --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
        result = subprocess.run(
            bash_command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    
    @task
    def etf_sector_aggregates (ETF_symbol: str):
        logger.info(f'etf_sector_aggregates : {ETF_symbol}')
        vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
        #'etf_sector_aggregates',        
        bash_command=f"dbt run -s etf_sector_aggregates --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
        result = subprocess.run(
            bash_command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    
    etf_symbols = fetch_unique_etfs()
    combined_etf_symbol = etf_tickers_combine.expand(ETF_symbol=etf_symbols)
    etf_top_ticker_prices.expand(ETF_symbol=combined_etf_symbol)
    etf_sector_aggregates.expand(ETF_symbol=combined_etf_symbol)


dag_instance = etf_transformations_dag()
