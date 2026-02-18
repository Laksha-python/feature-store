from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kafka_ingestion_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None, 
    catchup=False,
    default_args=default_args,
) as dag:

    run_kafka_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="cd /opt/feature_project && python -m ingestion.kafka_producer",
    )
