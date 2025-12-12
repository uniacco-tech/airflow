from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/uniacco' 
sys.path.append(SCRIPTS_PATH)

from uniacco_partner_lead_deduption import run_lead_dedupe_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="us_partner_lead_deduping", 
    default_args=default_args,
    description="Compares UniAcco Partner lead dump against Redshift for duplicates (phone/email)",
    schedule_interval="0 7,9,12 * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniAcco", "GoogleSheets"]
) as dag:

    dedupe_task = PythonOperator(
        task_id="us_partner_lead_deduping", 
        python_callable=run_lead_dedupe_etl
    )

    dedupe_task
