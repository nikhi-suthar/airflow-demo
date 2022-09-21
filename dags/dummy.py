from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import time

default_args = {
    'owner': 'Lumiq',
    'provide_context': True,
    'start_sla': timedelta(seconds=5),
    'run_sla': timedelta(seconds=10),
    'sla': timedelta(seconds=10)
}



def taskmethod(**kwargs):
    print(":::upstream_task_ids -- {}".format(str(kwargs['task'].upstream_task_ids)))
    print(":::downstream_task_ids  -- {}".format(str(kwargs['task'].downstream_task_ids )))
    print(":::::::::::Taks -- {}".format(kwargs['task']))
    print(":::::::::::Taks -- {}".format(kwargs['task'].task_id))
    print("::::::State --- {}".format(kwargs["dag_run"].get_task_instance(kwargs['task'].task_id).state))


def sleepMethod(**kwargs):
    print(":::::Start Task {}".format(kwargs["Name"]))
    print("Start Time: {}".format(str(datetime.now())))
    time.sleep(int(kwargs["sleepTime"]))
    print("End Time: {}".format(str(datetime.now())))
    name = int(kwargs['Name']) - 1
    kwargs['ti'].xcom_push(key=str(kwargs['Name']), value=kwargs["sleepTime"])
    while name != 0:
          print(" ----> " + str(kwargs['ti'].xcom_pull(key=str(name))))
          name = name - 1
    if int(kwargs["sleepTime"]) % 2 ==0:
        raise ValueError('Error in sleepTime')

@dag(default_args=default_args,start_date=datetime(2022, 9, 1), schedule_interval='*/5 * * * *', catchup=False)
def Dummy():
    A = PythonOperator(task_id="1", python_callable=taskmethod, op_kwargs={"Name": "1", "sleepTime":1})
    B = PythonOperator(task_id="2", python_callable=taskmethod, op_kwargs={"Name": "2", "sleepTime":3})
    C = PythonOperator(task_id="3", python_callable=taskmethod, op_kwargs={"Name": "3","sleepTime":5})
    D = PythonOperator(task_id="4", python_callable=taskmethod, op_kwargs={"Name": "4","sleepTime":7}, execution_timeout=timedelta(seconds=10))
    E = PythonOperator(task_id="5", python_callable=taskmethod, op_kwargs={"Name": "5", "sleepTime": 9}, sla=timedelta(seconds=5))

    A >> [B,C] >> D >> E
dag = Dummy()