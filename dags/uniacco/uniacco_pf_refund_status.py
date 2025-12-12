from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/uniacco' 
sys.path.append(SCRIPTS_PATH)

from uniacco_pf_refund_status import run_refund_status_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),}

with DAG(
    dag_id="uniacco_pf_refund_status", 
    default_args=default_args,
    description="Compares UniAcco PF Form data with payment status and updates refund status/date.",
    schedule_interval="5 */4 * * *", 
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniAcco", "GoogleSheets"]
) as dag:

    update_status_task = PythonOperator(
        task_id="uniacco_pf_refund_status", 
        python_callable=run_refund_status_etl
    )

    update_status_task
