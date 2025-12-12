from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/unischolars' 
sys.path.append(SCRIPTS_PATH)

from us_counsellor_red_flag import run_counsellor_red_flag_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="us_counsellor_red_flag", 
    default_args=default_args,
    description="Generates daily Counsellor Red Flag report on sales adherence and aging leads, and emails management.",
    schedule_interval="25 16 * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniScholars", "Report", "Email"]
) as dag:

    red_flag_task = PythonOperator(
        task_id="generate_and_send_counsellor_red_flag_report", 
        python_callable=run_counsellor_red_flag_etl
    )

    red_flag_task
