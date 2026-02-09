from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Folder where CSV will be stored
CSV_PATH = "/tmp/demo_airflow_data.csv"

def start_task():
    print("DAG execution started")

def create_csv():
    data = {
        "id": [1, 2, 3],
        "name": ["Airflow", "Docker", "Kubernetes"],
        "category": ["Orchestration", "Container", "Orchestration"]
    }

    df = pd.DataFrame(data)
    df.to_csv(CSV_PATH, index=False)
    print(f"CSV file created at {CSV_PATH}")

def read_csv():
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
        print("CSV file content:")
        print(df)
    else:
        print("CSV file not found")

def end_task():
    print("DAG execution completed")

# DAG definition
with DAG(
    dag_id="csv_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Manual trigger (best for demo)
    catchup=False,
    tags=["demo", "csv", "beginner"]
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task
    )

    create_csv_task = PythonOperator(
        task_id="create_csv",
        python_callable=create_csv
    )

    read_csv_task = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv
    )

    end = PythonOperator(
        task_id="end",
        python_callable=end_task
    )

    # Task dependencies
    start >> create_csv_task >> read_csv_task >> end
