import datetime
from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreatePysparkJobOperator
)


DP_CLUSTER_ID = "c9qik72cb8fjkjb4q6bb"
YC_BUCKET = 'dataproc-base-bucket'


with DAG(
        'DATA_INGEST',
        schedule_interval='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        cluster_id=DP_CLUSTER_ID,
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/calculate_air_quality_metrics.py',
    )

    poke_spark_processing
