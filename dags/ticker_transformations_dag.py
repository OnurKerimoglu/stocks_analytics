import os
import logging

from airflow.decorators import dag
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
    }
)
def ticker_transformations_dag():

    price_technicals = BashOperator(
        task_id='price_technicals_lastday',
        bash_command=f"dbt run -s price_technicals_lastday  --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"
    )

    price_technicals

dag_instance = ticker_transformations_dag()