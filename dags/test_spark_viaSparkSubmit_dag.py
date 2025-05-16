from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    schedule_interval=None,
    catchup=False
)
def test_spark_dag():
    test_spark_task = SparkSubmitOperator(
        task_id='test_spark',
        application='/opt/airflow/src/test_spark.py',
        conn_id='spark_standalone_client',
        # keytab='airflow',
        # principal='airflow',
        name=None,
        executor_cores=1,
        total_executor_cores=1,
        verbose=True
    )
    test_spark_task

test_spark_dag()
