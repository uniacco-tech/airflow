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
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def say_hello():
    print("Hello from pilot_test_dag!")
    return "hello"

def print_runtime_info(**context):
    print("=== Runtime context ===")
    print("dag_id:", context.get("dag").dag_id if context.get("dag") else "n/a")
    print("task_id:", context.get("task_instance").task_id if context.get("task_instance") else "n/a")
    print("execution_date:", context.get("execution_date"))
    print("Hostname:", socket.gethostname())
    print("ENV SAMPLE_ENV:", os.environ.get("SAMPLE_ENV", "<not set>"))
    # Example: push an XCom
    return {"hostname": socket.gethostname()}

with DAG(
    dag_id="pilot_test_dag",
    default_args=default_args,
    description="A small pilot DAG for testing on an EC2 Airflow instance",
    schedule_interval=None,   # manual trigger only
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
        provide_context=False,  # Airflow passes context to callables that accept **kwargs
    )

    # Simple linear workflow
    task_hello >> task_sleep >> task_print
