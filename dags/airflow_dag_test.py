#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder in path
sys.path.append(os.path.join(os.path.dirname(__file__), "/opt/airflow/dags/current/scripts/airflow_python_test.py"))

from etl_job import run_query # import your file by file name

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_bi",
    default_args=default_args,
    description="BI",
    schedule_interval="*/7 * * * *",  # Runs hourly (change as needed)
    start_date=datetime(2025, 12, 4),
    catchup=False,
) as dag:

    redshift_etl_task = PythonOperator(
        task_id="BI Test",
        python_callable=run_query
    )

redshift_etl_task

