from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/unischolars' 
sys.path.append(SCRIPTS_PATH)

from unischolars_live_counselling import run_live_ql_report_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="unischolars_live_counselling", 
    default_args=default_args,
    description="Extracts Live Qualified Leads data from Redshift and updates the Google Sheet report.",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniScholars", "GoogleSheets"]
) as dag:

    live_ql_task = PythonOperator(
        task_id="unischolars_live_counselling", 
        python_callable=run_live_ql_report_etl
    )

    live_ql_task
