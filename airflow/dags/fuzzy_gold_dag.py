from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import sys



AIRFLOW_PROJECT_ROOT = '/opt/airflow'
if AIRFLOW_PROJECT_ROOT not in sys.path:
    sys.path.append(AIRFLOW_PROJECT_ROOT)


from src.fuzzy_match import main as fuzzy_gold_main


def _run_fuzzy_gold_script(**kwargs):
    kwargs['ti'].log.info(f"Running fuzzy_gold_main for DAG run {kwargs['dag_run'].run_id} at {datetime.now()}")   
    fuzzy_gold_main()
    kwargs['ti'].log.info(f"fuzzy_gold_main completed.")




with DAG(
    dag_id='fuzzy_gold_dag',
    start_date=days_ago(1),
    schedule_interval='0 2 * * *', 
    catchup=False,
    tags=['gold', 'api', 'snowflake']
) as dag:

    run_fuzzy_gold = PythonOperator(
        task_id='run_fuzzy_gold',
        python_callable=_run_fuzzy_gold_script,
        provide_context=True,
    )
