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
def transformation_dag():
    ETF_symbol = '{{ params.ETF_symbol }}'
    logger.info(f'Running the ingest_raw_data_dag for {ETF_symbol}')

    # holding_counts = BashOperator(
    #     task_id='holding_counts',
    #     # bash_command='dbt build - models stocks.etf_holdings'
    #     bash_command=f"dbt run -s etf_holding_counts --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
    # )
    vararg = r'{\"etf_symbol\": ' + r'\"{}\"'.format(ETF_symbol) + r'}'
    etf_ticker_weights = BashOperator(
        task_id='etf_ticker_weights',
        # bash_command='dbt build - models stocks.etf_holdings'
        # dbt run --select filtered_table --vars '{"etf_symbol": "IVV"}'
        bash_command=f"dbt run -s etf_ticker_weights --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
    )

    # holding_counts
    etf_ticker_weights

dag_instance = transformation_dag()
