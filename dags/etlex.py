from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extract():
    data = {
        "order_id": [1, 2, 3],
        "price": [100, 200, 150],
        "quantity": [2, 1, 3]
    }
    df = pd.DataFrame(data)
    df.to_csv("/tmp/raw_orders.csv", index=False)

def transform():
    df = pd.read_csv("/tmp/raw_orders.csv")
    df["total_amount"] = df["price"] * df["quantity"]
    df.to_csv("/tmp/processed_orders.csv", index=False)

def load():
    df = pd.read_csv("/tmp/processed_orders.csv")
    print("Final data loaded:")
    print(df)

with DAG(
    dag_id="etl_sales_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_orders",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_orders",
        python_callable=load
    )

    extract_task >> transform_task >> load_task
