from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/unicreds'
sys.path.append(SCRIPTS_PATH)

from unicreds_google_leads_upload import run_conversion_tracking_etl

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="unicreds_google_leads_upload", 
    default_args=default_args,
    description="Extracts UniCreds Qualified and Caterable leads for Google Ads offline conversion tracking.",
    schedule_interval="30 13 * * *",
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniCreds", "GoogleSheets", "Marketing"]
) as dag:

    conversion_tracking_task = PythonOperator(
        task_id="generate_and_upload_all_conversion_reports", 
        python_callable=run_conversion_tracking_etl
    )

    conversion_tracking_task
