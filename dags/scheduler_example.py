from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def log_message():
    print("Scheduled DAG executed successfully")

with DAG(
    dag_id="weekday_10min_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * 1-5",  # Every 10 minutes, Mon–Fri
    catchup=False,
    tags=["schedule", "demo"]
) as dag:

    log_task = PythonOperator(
        task_id="log_message",
        python_callable=log_message
    )

    log_task
