from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/uniacco' 
sys.path.append(SCRIPTS_PATH)

from uniacco_initiation_ops import final_fun

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="uniacco_initiation_ops", 
    default_args=default_args,
    description="Updating UniAcco Booking Initiation Dump",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniAcco", "Report", "GoogleSheets"]
) as dag:

    inventory_report_task = PythonOperator(
        task_id="uniacco_initiation_ops", 
        python_callable=run_inventory_report_etl
    )

    inventory_report_task
