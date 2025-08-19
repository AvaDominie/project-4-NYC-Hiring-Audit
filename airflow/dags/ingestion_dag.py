from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import sys



AIRFLOW_PROJECT_ROOT = '/opt/airflow'
if AIRFLOW_PROJECT_ROOT not in sys.path:
    sys.path.append(AIRFLOW_PROJECT_ROOT)


from src.data_acquisition import main as data_ingestion_main

# Add a check for the last--modified date of the data in the source API before running the ingestion script.
# Cleanup logs to limit to most recent 5 files


def _run_data_ingestion_script(**kwargs):
    kwargs['ti'].log.info(f"Running data_ingestion_main for DAG run {kwargs['dag_run'].run_id} at {datetime.now()}")
    data_ingestion_main()
    kwargs['ti'].log.info(f"data_ingestion_main completed.")





with DAG(
    dag_id='data_ingestion_pipeline',
    start_date=days_ago(1),
    schedule_interval='0 2 * * *', 
    catchup=False,
    tags=['ingestion','bronze', 'api', 'minio']
) as dag:
    pull_api_data_to_minio_to_snowflake_task = PythonOperator(
        task_id='pull_api_data_to_minio_to_snowflake',
        python_callable=_run_data_ingestion_script,
        provide_context=True
    )