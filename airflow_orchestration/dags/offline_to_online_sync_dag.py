from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="offline_to_online_sync_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    sync_offline_to_online = BashOperator(
        task_id="sync_offline_to_online",
        bash_command="cd /opt/feature_project && python -m jobs.offline_to_online_sync",
    )
