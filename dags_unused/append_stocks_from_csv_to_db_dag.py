from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.db_functions import insert_data

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}
with DAG(
    dag_id="append_stocks_from_csv_to_db_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['manual'],
) as dag:
    insert_from_csv_into_table = PythonOperator(
        task_id="insert_from_csv_into_table_task",
        python_callable=insert_data,
        op_kwargs={
            "table_name": 'stocks_to_track',
            "init_stock_file": '/opt/airflow/data/default_stocks.csv'
        },
    )

insert_from_csv_into_table
