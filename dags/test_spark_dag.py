from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator 


@dag(
    schedule_interval=None,
    catchup=False
)
def test_spark_dag():

    test_remote_spark_job = SSHOperator(
        task_id="run_spark_job",
        ssh_conn_id="spark_ssh_connection",
        command="/home/onur/opt/spark/spark-3.5.5-bin-hadoop3/bin/spark-submit --master spark://L54Ku2004:7077 /home/onur/WORK/DS/repos/stocks_analytics/src/test_spark.py",
    )
    test_remote_spark_job

test_spark_dag()
