from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10, 14, 0),
    'email': ['cjs@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
} 

dag = DAG(
    'cjs_tutorial_bash', #DAG_ID , 중복되지 않아야 한다.
    default_args=default_args,
    description='My first tutorial bash DAG',
    schedule_interval= '* * * * *' ,
    catchup=False
) 

t1 = BashOperator(
    task_id='say_hello',
    bash_command='echo "hello world"',
    dag=dag 
) 

t2 = BashOperator(
    task_id='what_time',
    bash_command='date',
    dag=dag 
) 

t1 >> t2 
