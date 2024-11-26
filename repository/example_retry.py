from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import random
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG('example_retry',
    default_args=default_args,
    catchup=False
    )

def random_exception_task_1(**context):
    print("random_exception_task_1")
    val = random.choice([1,2,3])
    #val = random.choice(range(3))
    print("val : "+val)
    if val == 1:
        context['task_instance'].xcom_push(key='failValue', value=val)
        #raise AirflowFailException("Fail task")
        raise Exception()
    
def random_exception_task_2(**context):
    print("random_exception_task_2")
    val = random.choice([1,2,3])
    #val = random.choice(range(3))
    print("val : "+val)
    if val == 1:
        context['task_instance'].xcom_push(key='failValue', value=val)
        #raise AirflowFailException("Fail task")
        raise Exception()

def func1():
    print("f")

t2 = BashOperator(
    task_id="t2",
    bash_command="echo I get 6 retries and never wait long! && False",
    retries=6,
    max_retry_delay=timedelta(seconds=10),
)

test = PythonOperator(
    task_id='test',
    python_callable=func1,
    retries=5,
    retry_delay=timedelta(minutes=3),
    dag=dag
)

task = PythonOperator(
    task_id='sample_retry',
    python_callable=random_exception_task_1,
    retries=5,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

task2 = PythonOperator(
    task_id='sample_retry2',
    python_callable=random_exception_task_2,
    retries=5,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

t2 >> test >> task >> task2