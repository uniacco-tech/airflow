from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Define the directory where your Python files are located
# IMPORTANT: Replace this with the actual path on your Airflow worker machine.
# If the files are in the same directory as the DAG, you can often omit this.
SCRIPT_DIR = '/path/to/your/unischolars/scripts/' 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="unischolars_standard_etl",
    default_args=default_args,
    description="Runs UniScholars Standard ETL followed by Application Standard ETL.",
    schedule_interval="14 */2 * * *",
    catchup=False,
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    tags=["unicreds", "etl", "standard"],
) as dag:

    # --- Task 1: Run unischolars_standard.py ---
    task_standard = BashOperator(
        task_id="run_unischolars_standard",
        bash_command=f"python {os.path.join(SCRIPT_DIR, 'unischolars_standard.py')}",
    )

    # --- Task 2: Run unischolars_application_standard.py ---
    task_application_standard = BashOperator(
        task_id="run_unischolars_application_standard",
        bash_command=f"python {os.path.join(SCRIPT_DIR, 'unischolars_application_standard.py')}",
    )

    task_standard >> task_application_standard
