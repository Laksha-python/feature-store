from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 0,
}

with DAG(
    dag_id="feature_validation_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    validate_features = BashOperator(
        task_id="validate_features",
        bash_command="cd /opt/feature_project && python -m jobs.validate_online_features",
    )
