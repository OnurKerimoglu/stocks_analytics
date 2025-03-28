from datetime import datetime
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlalchemy

PG_HOST = os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

def insert_data(init_stock_file, table_name):
    logging.info(f"inserting values from {init_stock_file} into {table_name}")

    engine = sqlalchemy.create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')
    engine.connect()

    df = pd.read_csv(init_stock_file)
    df['added_by'] = 'system'
    df['added_date'] = pd.to_datetime(datetime.now())
    df.set_index('symbol', inplace=True)

    if sqlalchemy.inspect(engine).has_table(table_name):
        df_existing = pd.read_sql(f"SELECT * from {table_name}", engine)
        logging.info(f'found {df.shape[0]} existing rows in {table_name}')
        # remove the rows that already exists in the table from the df
        df_existing.set_index('symbol', inplace=True)
        df = df[~df.index.isin(df_existing.index)]
    else:
        df.head(n=0).to_sql(name=table_name, con=engine)

    # insert remaining values into the table
    if df.empty:
        logging.info('no new values to insert')
    else:
        df.to_sql(name=table_name, con=engine, if_exists='append')
        logging.info(f'inserted {df.shape[0]} rows')

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
