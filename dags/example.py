from airflow.decorators import dag, task
from time import sleep
from datetime import datetime

@dag(
    dag_id="celery_parallel_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def celery_dag():
    
    @task
    def a():
        sleep(5)
    
    @task
    def b():
        sleep(5)


    @task
    def c():
        sleep(5)
    
    @task
    def d():
        sleep(5)

    
    a() >> [b() , c()] >> d()

celery_dag()