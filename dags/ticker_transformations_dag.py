import os
import logging
import subprocess

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from src.shared import config_logger

config_logger('info')
logger = logging.getLogger(__name__)

rootpath = os.environ.get("AIRFLOW_HOME")
dbt_dir = os.path.join(rootpath, 'dbt', 'stocks_dbt')
DEFAULT_ENV = "prod"

def decide_branch(**kwargs):
    env = kwargs['params'].get('env')
    if env == 'upstream':
        return 'pull_env'
    else:
        return 'skip_pull_env'

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
        'env': Param(
            "upstream",
            type="string",
            title="environment",
            enum=["dev", "prod", "upstream"]
        )
    }
)
def ticker_transformations_dag():

    branch = BranchPythonOperator(
        task_id='branch_env',
        python_callable=decide_branch,
        provide_context=True,
    )

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
        execution_date = context['execution_date']
        if not env:
            logger.error(f"No env value was found in {dag_id}/{task_ids} XCom for execution_date: {execution_date}")
        else:
            logger.info(f"Pulled env from {dag_id}/{task_ids} XCom for execution_date: {execution_date}: {env}")
        context['ti'].xcom_push(key='upstream_env', value=env)

    skip_pull_env_task = EmptyOperator(
        task_id='skip_pull_env'
        )
    
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def resolve_env(**context):
        ti = context['ti']
        env_param = context['params'].get('env')
        if env_param == 'upstream':
            resolved_env = ti.xcom_pull(task_ids='pull_env', key='upstream_env')
            logger.info(f"Setting upstram env: {resolved_env}")
            if not resolved_env:
                resolved_env = DEFAULT_ENV
                logger.info(f"Falling back to default env: {DEFAULT_ENV}")
        else:
            resolved_env = env_param
            logger.info(f"Setting conf env: {resolved_env}")
        ti.xcom_push(key='resolved_env', value=resolved_env)

    @task
    def price_technicals_lastday(**context):
        env = context['ti'].xcom_pull(key='resolved_env', task_ids='resolve_env')
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
        reset_dag_run=True,
        wait_for_completion=False,
        deferrable=False
    )
    
    pull_env_task = pull_env()
    resolve_env_task = resolve_env()
    price_technicals_lastday_task = price_technicals_lastday()

    branch >> [pull_env_task, skip_pull_env_task] >> resolve_env_task
    resolve_env_task >> price_technicals_lastday_task
    price_technicals_lastday_task >> triggered_etf_transformations_dag

dag_instance = ticker_transformations_dag()