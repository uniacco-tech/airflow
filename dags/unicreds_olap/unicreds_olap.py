from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/git/current/script/unicreds_olap' 
sys.path.append(SCRIPTS_PATH)

from unicreds_olap import run_all_etl_steps 

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="uc_standard_table", 
    default_args=default_args,
    description="ETL for UniCreds Data Warehouse tables (UC_Lead_2, UC_Lead_3, etc.)",
    schedule_interval="35 0-23/2 * * *", 
    start_date=datetime(2025, 12, 12),
    max_active_runs=1,
    catchup=False,
    tags=["UniCreds", "ETL", "Table"]
) as dag:

    redshift_etl_task = PythonOperator(
        task_id="uc_standard_table", 
        python_callable=run_all_etl_steps
    )
    
    redshift_etl_task
