from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/uniacco' 
sys.path.append(SCRIPTS_PATH)

from uniacco_pf_refund import run_refund_email_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="uniacco_pf_refund", 
    default_args=default_args,
    description="Checks PF refund Form for new refund requests, sends approval emails, and updates tracker sheet.",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniAcco", "Email"]
) as dag:

    email_notification_task = PythonOperator(
        task_id="uniacco_pf_refund", 
        python_callable=run_refund_email_etl
    )

    email_notification_task
