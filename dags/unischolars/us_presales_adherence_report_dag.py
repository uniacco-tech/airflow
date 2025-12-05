#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder in path
sys.path.append('/opt/airflow/dags/current/script/unischolars')

# import your file by file name
from us_presales_adherence_report import run_query 

default_args = {
    "owner": "nishant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="us_pre_sales_adherence_report",
    default_args=default_args,
    description="Pre Sales Adherence report of Pre Sales team",
    schedule_interval="*/5 * * * *",  # time update (change as needed)
    start_date=datetime(2025, 12, 4),
    catchup=False,
    tags=["UniScholars", "Report","Email"]
) as dag:

    redshift_etl_task = PythonOperator(
        task_id="us_presales_adherence_report",
        python_callable=run_query
    )

redshift_etl_task

