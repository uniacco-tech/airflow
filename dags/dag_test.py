#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime as dt
import logging
import redshift_connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DB = "dev"
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y"
REDSHIFT_PORT = 5439

def run_query():
    logging.info("Connecting to Redshift...")

    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DB,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    truncate_sql = "TRUNCATE TABLE test;"
    insert_sql = """
        INSERT INTO test
        SELECT * FROM uainventory.leads_lead
        WHERE date(created_at + interval '330' minute) = date(current_timestamp + interval '330' minute);
    """

    with conn.cursor() as cur:
        logging.info("Executing TRUNCATE...")
        cur.execute(truncate_sql)

        logging.info("Executing INSERT...")
        cur.execute(insert_sql)

    conn.commit()
    conn.close()
    logging.info("ETL completed successfully.")


default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2021, 3, 16, 0, 0),
    "retries": 0
}

with DAG(
    "dag_test_table",
    default_args=default_args,
    schedule_interval="*/12 * * * *",  # Runs daily at 9 AM IST
    catchup=False,
    description="Daily ETL for test table refresh",
    tags=["Redshift", "ETL"]
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting ETL process...'"
    )
    
    run_etl = PythonOperator(
        task_id="run_redshift_query",
        python_callable=run_query
    )

    start >> run_etl

