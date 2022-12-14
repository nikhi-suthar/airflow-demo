from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Nikhil',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='New_Dag',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2022, 9, 19),
    schedule_interval='@Daily',
    is_paused_upon_creation=True
) as dag:
    A = BashOperator(
        task_id='A',
        bash_command="airflow variables import /home/airflow/extra/properties.json"
    )

    B = BashOperator(
        task_id='B',
        bash_command="echo hey, I am task2 and will be running after task1!"
    )

    C = BashOperator(
        task_id='C',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )

    D = BashOperator(
        task_id='D',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )

    A >> [B, C] >> D