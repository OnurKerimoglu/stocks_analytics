import os
import logging

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from src.shared import config_logger

config_logger('info')
logger = logging.getLogger(__name__)

rootpath = os.environ.get("AIRFLOW_HOME")
dbt_dir = os.path.join(rootpath, 'dbt', 'stocks_dbt')

@dag(
    schedule=None,
    start_date=days_ago(1), 
    catchup=False,
    default_args={
        "owner": "Onur",
        "retries": 1,
        "retry_delay": 5
    },
    params={
        'ETF_symbol': Param(
            'IVV',
            type='string',
            title='ETF Ticker symbol'
        )
    }
)
def etf_transformations_dag():
    ETF_symbol = '{{ params.ETF_symbol }}'
    logger.info(f'Running the ingest_raw_data_dag for {ETF_symbol}')

    vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
    etf_ticker_weights = BashOperator(
        task_id='etf_tickers_combine',     
        bash_command=f"dbt run -s etf_tickers_combine --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
    )
    
    etf_top_ticker_prices = BashOperator(
        task_id='etf_top_ticker_prices',     
        bash_command=f"dbt run -s etf_top_ticker_prices --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
    )

    etf_sector_aggregates = BashOperator(
        task_id='etf_sector_aggregates',        
        bash_command=f"dbt run -s etf_sector_aggregates --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
    )

    etf_ticker_weights >> etf_sector_aggregates
    etf_ticker_weights >> etf_top_ticker_prices


dag_instance = etf_transformations_dag()
