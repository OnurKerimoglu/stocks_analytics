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
from src.fetch_forecast import FetchForecast
from src.shared import config_logger
from src.gc_functions import get_data_from_bq_operator, upload_to_gcs

config_logger('info')
logger = logging.getLogger(__name__)

rootpath = os.environ.get("AIRFLOW_HOME")
credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
API_URL_TEMPLATE = os.environ.get("API_URL_TEMPLATE")
dbt_dir = os.path.join(rootpath, 'dbt', 'stocks_dbt')
DEFAULT_ENV = "prod"

def fetch_file_paths(dirpath, ext):
    # construct a list of absolute paths based on the 
    # files with the specified extension in the self.datapth folder
    fpaths = []
    for f in os.listdir(dirpath):
        if f.endswith(f'.{ext}'):
            fpaths.append(os.path.join(dirpath, f))
    return fpaths

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
        ),
        'api_env': Param(
            "prod",
            type="string",
            title="API environment",
            enum = ["dev", "prod"],
        )
    }
)
def etf_forecasts_dag():

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
    def fetch_forecasts(ETF_symbol: str, DWH: dict, **context):
        api_env = context['params'].get('api_env')
        api_url = API_URL_TEMPLATE.replace("ENV", api_env)
        logger.info(f'Getting historical data for {ETF_symbol}')
        df_hist = get_data_from_bq_operator(
            credentials_path,
            f"SELECT date, close FROM {DWH['DS_raw']}.{DWH['T_prices']} WHERE symbol = '{ETF_symbol}'"
            )
        df_hist.rename(columns={'date': 'Date', 'close': 'Close'}, inplace=True)
        logger.info(f'Fetching forecast for {ETF_symbol}')
        fpath = FetchForecast(api_url, ETF_symbol, df_hist).run()
        return fpath
    
    @task
    def ul_to_gcs(DWH: dict, fpath: str, source: str = 'forecast'):
        logger.info(f"Uploading {fpath} in GCS")
        object_name=f"{source}/{os.path.basename(fpath)}"
        if os.path.exists(fpath):
            logger.info(f"Uploading {fpath} to {object_name}")
            upload_to_gcs(
                bucket = DWH['bucket'],
                object_name=object_name,
                local_file=fpath)
        else:
            logger.warning(f"{fpath} does not exist")
        return object_name
    
    @task
    def append_to_bq(DWH: dict, object_name: str):
        fname = os.path.basename(object_name)
        logger.info(f'Appending {fname} to {DWH["DS_raw"]}.{DWH["T_forecasts"]}')
        return fname

    @task
    def remove_local(source: str, fname: str):
        fpath = fetch_file_paths(
            os.path.join(rootpath, 'data', source, fname))
        logger.info(f"Removing {fpath}")
        # os.remove(fpath)
        return fpath
    
    pull_env_task = pull_env()
    resolve_env_task = resolve_env()
    DWH = load_configs_task()
    branch >> [pull_env_task, skip_pull_env_task] >> resolve_env_task >> DWH
    
    etf_symbols = fetch_unique_etfs(DWH)    
    fpaths = fetch_forecasts.partial(DWH=DWH).expand(ETF_symbol=etf_symbols)
    ul_object_names = ul_to_gcs.partial(DWH=DWH).expand(fpath=fpaths)
    ap_fnames = append_to_bq.partial(DWH=DWH).expand(object_name=ul_object_names)
    remove_task = remove_local.partial(source='forecast').expand(fname=ap_fnames)
    etf_symbols >> fpaths >> ul_object_names >> ap_fnames >> remove_task

dag_instance = etf_forecasts_dag()
