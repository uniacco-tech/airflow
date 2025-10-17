from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="pilot_test_dag",
    start_date=datetime(2025, 10, 16),
    schedule=None,   # manual runs only
    catchup=False,
    tags=["pilot"],
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command='echo "hello from pilot_test_dag ($(hostname))"',
    )
