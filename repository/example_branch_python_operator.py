import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'start_date': datetime(2021, 7, 31),
    'schedule_interval': '@daily',
    'catchup': False
}

def choose_branch(**kwargs):
    branches = ['b1', 'b2', 'b3']
    chosen = random.choice(branches)
    print(f'chosen: {chosen}')
    return chosen

with DAG(dag_id='example_branch_python_operator', default_args=default_args, schedule_interval=None) as dag:
    start_dag = BashOperator(task_id='start', bash_command='echo start')

    branching = BranchPythonOperator(task_id='choose_branch', python_callable=choose_branch)
    b1 = BashOperator(task_id='b1', bash_command='echo b1')
    b2 = BashOperator(task_id='b2', bash_command='echo b2')
    b3 = BashOperator(task_id='b3', bash_command='echo b3')
    c1 = BashOperator(task_id='c1', bash_command='echo c1')

    start_dag >> branching >> [b1, b2, b3]
    b1 >> c1
