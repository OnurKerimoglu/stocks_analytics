import os
import logging
import subprocess

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

    @task
    def pull_env(**context):
        # Pull the value from ingestion dag's task instance
        task_ids = 'set_push_env'
        dag_id = 'ingest_raw_data_dag'
        env = context['ti'].xcom_pull(
            task_ids=task_ids,
            key='env',
            dag_id = dag_id
            )
        logger.info(f"Pulled env from {dag_id}/{task_ids} XCom: {env}")
        return env

    @task
    def price_technicals_lastday(env: str):
        logger.info(f'running price_technicals_lastday for {env} environment')
        bash_command=f"dbt run -s price_technicals_lastday  --profiles-dir {dbt_dir}/config --project-dir {dbt_dir} --target {env}"
        logger.info(f'running command: {bash_command}')
        result = subprocess.run(
            bash_command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    
    triggered_etf_transformations_dag = TriggerDagRunOperator(
        trigger_dag_id="etf_transformations_dag",
        task_id="triggered_etf_transformations_dag",
        execution_date="{{ execution_date }}",
        wait_for_completion=False,
        deferrable=False
    )
    
    env = pull_env()
    price_technicals_lastday(env) >> triggered_etf_transformations_dag

dag_instance = ticker_transformations_dag()