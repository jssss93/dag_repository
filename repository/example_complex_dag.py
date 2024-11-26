import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

default_args = {
    'start_date': datetime(2021, 7, 31),
    'schedule_interval': '@daily'
}

def choose_branch(**kwargs):
    branches = ['b1', 'b2', 'b3']
    chosen = random.choice(branches)
    print(f'chosen: {chosen}')
    return chosen

def choose_branch_from_httpOperator(ti):
    nextTask = ti.xcom_pull(key='return_value', task_ids=['extracting_user1'])
    print('NEXT TASK###')
    print(nextTask)
    return nextTask

def choose_branch_from_podOperator(ti):
    jsonData = ti.xcom_pull(key='return_value', task_ids=['kubernetespodoperator'])
    print('jsonData###')
    print(jsonData)
    print(jsonData[0].get('res'))
    
    return jsonData[0].get('res')

with DAG(dag_id='example_complex_dag.py', default_args=default_args, schedule_interval=None) as dag:
    start_dag = BashOperator(task_id='start', bash_command='echo start')

    branching = BranchPythonOperator(task_id='choose_branch', python_callable=choose_branch)
    b1 = BashOperator(task_id='b1', bash_command='echo b1')
    b2 = BashOperator(task_id='b2', bash_command='echo b2')
    b3 = BashOperator(task_id='b3', bash_command='echo b3')
    B1 = BashOperator(task_id='B1', bash_command='echo B1')
    B2 = BashOperator(task_id='B2', bash_command='echo B2')
    B3 = BashOperator(task_id='B3', bash_command='echo B3')
    c1 = BashOperator(task_id='c1', bash_command='echo c1')


    t1 = SimpleHttpOperator(
        task_id='extracting_user1',
        http_conn_id='sample',
        endpoint='/getRandNum',
        method='GET',
        #response_filter=lambda response: json.loads(response.text),
        log_response=True,
        do_xcom_push=True,
        trigger_rule="one_success",
    	dag=dag,
    )

    run = KubernetesPodOperator(
        task_id="kubernetespodoperator",
        namespace='airflow',
        image='cjs0533/batch_example:0.0.8',
        name="job",
        is_delete_operator_pod=True, #Pod operator가 동작하고 난 후 삭제
        get_logs=True, #Pod의 동작하는 로그
        dag=dag,
        arguments=["--job.name=test"],
        do_xcom_push=True,
        trigger_rule="one_success",
    )
        

    choose_branch_from_httpOperator = BranchPythonOperator(
        task_id='choose_branch_from_httpOperator',
        python_callable=choose_branch_from_httpOperator
    )

    choose_branch_from_podOperator = BranchPythonOperator(
        task_id='choose_branch_from_podOperator',
        python_callable=choose_branch_from_podOperator
    )
    
    start_dag >> t1 >> choose_branch_from_httpOperator >> [b1, b2, b3] >> run >> choose_branch_from_podOperator >> [B1, B2, B3]
    b1 >> c1
