from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="feature_platform_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="cd /opt/feature_project && python -m ingestion.kafka_producer",
    )

    run_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command="cd /opt/feature_project && python -m streaming.kafka_consumer",
    )

    run_spark = BashOperator(
        task_id="run_spark_features",
        bash_command="docker exec spark /opt/spark/bin/spark-submit /app/processing/spark_feature_job.py",
    )

    sync_online = BashOperator(
        task_id="sync_offline_to_online",
        bash_command="cd /opt/feature_project && python -m jobs.offline_to_online_sync",
    )

    update_freshness = BashOperator(
        task_id="update_freshness",
        bash_command="cd /opt/feature_project && python -m jobs.update_freshness",
    )

    validate = BashOperator(
        task_id="validate_features",
        bash_command="cd /opt/feature_project && python -m jobs.validate_online_features",
    )

    run_producer >> run_consumer >> run_spark >> sync_online >> update_freshness >> validate
