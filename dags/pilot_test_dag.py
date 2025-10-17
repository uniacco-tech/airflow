# dags/pilot_test_dag.py
from datetime import datetime, timedelta
import socket
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def say_hello():
    print("Hello from pilot_test_dag!")
    return "hello"

def print_runtime_info(**kwargs):
    ti = kwargs.get("ti")
    print("=== Runtime context ===")
    print("dag_id:", kwargs.get("dag").dag_id if kwargs.get("dag") else "n/a")
    print("task_id:", ti.task_id if ti else "n/a")
    print("execution_date:", kwargs.get("execution_date"))
    print("Hostname:", socket.gethostname())
    print("ENV SAMPLE_ENV:", os.environ.get("SAMPLE_ENV", "<not set>"))
    return {"hostname": socket.gethostname()}

with DAG(
    dag_id="pilot_test_dag",
    default_args=default_args,
    description="A tiny pilot DAG for manual testing",
    schedule=None,            # manual trigger only (use schedule="@daily" or cron if you want periodic)
    start_date=datetime(2025, 10, 16),
    catchup=False,
    tags=["pilot", "test"],
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    task_sleep = BashOperator(
        task_id="sleep_5_seconds",
        bash_command="sleep 5",
    )

    task_print = PythonOperator(
        task_id="print_runtime_info",
        python_callable=print_runtime_info,
    )

    task_hello >> task_sleep >> task_print
