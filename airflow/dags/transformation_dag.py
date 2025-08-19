import sys
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


PROJECT_ROOT_IN_AIRFLOW = '/opt/airflow'
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

SRC_PATH = os.path.join(os.path.dirname(__file__), '../../src')
SRC_PATH = os.path.abspath(SRC_PATH)
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)


with DAG(
    dag_id="silver_data_dag",
    start_date=days_ago(1),
    schedule_interval="30 2 * * *",
    catchup=False,
    tags=['dbt', 'silver', 'PAYROLL', 'transformation'],
) as dag:
    silver_data = BashOperator(
        task_id='silver_data',
        bash_command='cd /workspaces/project-4-NYC-Hiring-Audit/my_dbt_project4 && dbt run --select silver',
    )

