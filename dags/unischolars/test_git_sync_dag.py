from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print("🔥 Git-sync test DAG loaded successfully! If you're seeing this, your setup works.")


with DAG(
    dag_id="git_sync_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",   # runs once, safe for testing
    catchup=False,
    tags=["test", "git-sync"],
) as dag:

    test_task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )
