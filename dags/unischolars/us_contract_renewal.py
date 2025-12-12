from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/unischolars' 
sys.path.append(SCRIPTS_PATH)

from us_contract_renewal import run_contract_renewal_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="us_contract_renewal", 
    default_args=default_args,
    description="Checks contract renewal dates, sends email reminders, and updates a mail checker tracker.",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniScholars", "Email"]
) as dag:

    renewal_reminder_task = PythonOperator(
        task_id="us_contract_renewal", 
        python_callable=run_contract_renewal_etl
    )

    renewal_reminder_task
