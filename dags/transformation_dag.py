from airflow.decorators import dag
from airflow.models import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup



from datetime import datetime

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
def transformation_dag():

    holding_counts = BashOperator(
        task_id='holding_counts',
        # bash_command='dbt build - models stocks.etf_holdings'
        bash_command='dbt run --select etf_holding_counts --profiles-dir /opt/airflow/dbt/stocks'
    )

    holding_counts

dag_instance = transformation_dag()
