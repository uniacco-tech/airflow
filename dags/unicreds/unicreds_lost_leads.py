from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/unicreds'
sys.path.append(SCRIPTS_PATH)

from unicreds_lost_leads import run_lost_lead_dump_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="unicreds_lost_leads", 
    default_args=default_args,
    description="Generates daily UniCreds, UniScholars, and UniAcco lost lead data dumps for key lost reasons and opportunity leads.",
    schedule_interval="30 3 */2 * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniCreds", "Email"]
) as dag:

    lost_lead_dump_task = PythonOperator(
        task_id="generate_and_send_lost_lead_dump", 
        python_callable=run_lost_lead_dump_etl
    )

    lost_lead_dump_task
