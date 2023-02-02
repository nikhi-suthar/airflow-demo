from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'lumiq',
    'start_date': datetime(2022, 11, 12),
     'execution_timeout': timedelta(seconds=60)
}


with DAG(
    dag_id='Execution_Dag',
    default_args=default_args,
    schedule_interval='*/2 * * * *'
) as dag: 

 task1 = BashOperator(
        task_id='first_task',
        bash_command="sleep 100",
        dag=dag)

task1
