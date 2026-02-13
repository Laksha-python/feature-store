from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_feature_computation_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,  
    catchup=False,
    default_args=default_args,
) as dag:

    run_spark_feature_job = BashOperator(
        task_id="run_spark_feature_job",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "/app/processing/spark_feature_job.py"
        ),
    )
