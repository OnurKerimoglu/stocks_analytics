import os
import subprocess
import logging

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from src.config import load_configs
from src.shared import config_logger
from src.gc_functions import get_data_from_bq_operator

config_logger('info')
logger = logging.getLogger(__name__)

rootpath = os.environ.get("AIRFLOW_HOME")
credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
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
def etf_transformations_dag():

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
    def load_configs_task(**context):
        # Read the input parameter env
        env = context['ti'].xcom_pull(task_ids='resolve_env', key='resolved_env')
        logger.info(f'Retrieved env: {env}. Loading the configs accordingly')
        # Load configs and return the DWH dictionary
        CONFIG = load_configs(
            config_paths={
                'dwh': os.path.join(rootpath, 'config', 'dwh.yaml')
                },
            env=env
            )
        return CONFIG['DWH']
    
    @task
    def fetch_unique_etfs(DWH):
        logger.info(f'Fetching unique ETF symbols from {DWH['DS_raw']}.{DWH['T_etfs']} table')
        df = get_data_from_bq_operator(
            credentials_path,
            f"SELECT DISTINCT(fund_ticker) FROM {DWH['DS_raw']}.{DWH['T_etfs']}"
        )
        symbols = list(df['fund_ticker'])
        logger.info(f'Returnig {len(symbols)} unique symbols: {symbols}')
        return symbols

    @task
    def etf_tickers_combine(ETF_symbol: str, **context):
        env = context['ti'].xcom_pull(task_ids='resolve_env', key='resolved_env')
        logger.info(f'etf_tickers_combine: {ETF_symbol}')
        vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
        bash_command=f"dbt run -s etf_tickers_combine --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir} --target {env}"
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
    def etf_top_ticker_prices(ETF_symbol: str, **context):
        env = context['ti'].xcom_pull(task_ids='resolve_env', key='resolved_env')
        logger.info(f'etf_top_ticker_prices: {ETF_symbol}')
        vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
        # 'etf_top_ticker_prices',     
        bash_command=f"dbt run -s etf_top_ticker_prices --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir} --target {env}"
        result = subprocess.run(
            bash_command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    
    @task
    def etf_sector_aggregates (ETF_symbol: str, **context):
        env = context['ti'].xcom_pull(task_ids='resolve_env', key='resolved_env')
        logger.info(f'etf_sector_aggregates : {ETF_symbol}')
        vararg = r'{etf_symbol: ' + f"{ETF_symbol}" + r'}'
        #'etf_sector_aggregates',        
        bash_command=f"dbt run -s etf_sector_aggregates --vars '{vararg}' --profiles-dir {dbt_dir}/config --project-dir {dbt_dir} --target {env}"
        result = subprocess.run(
            bash_command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    
    pull_env_task = pull_env()
    resolve_env_task = resolve_env()
    DWH = load_configs_task()
    branch >> [pull_env_task, skip_pull_env_task] >> resolve_env_task >> DWH
    
    etf_symbols = fetch_unique_etfs(DWH)    
    combined_etf_symbol = etf_tickers_combine.expand(ETF_symbol=etf_symbols)
    etf_top_ticker_prices.expand(ETF_symbol=combined_etf_symbol)
    etf_sector_aggregates.expand(ETF_symbol=combined_etf_symbol)

dag_instance = etf_transformations_dag()
