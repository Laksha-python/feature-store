from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="raw_event_consumer_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    run_kafka_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command="cd /opt/feature_project && python -m streaming.kafka_consumer",
    )
