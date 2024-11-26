from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),
    'schedule_interval': '@daily',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('example_http_operator',
           default_args=default_args,
           catchup=False,
           max_active_runs=3)

dag.doc_md = __doc__

t1 = SimpleHttpOperator(
        task_id='extracting_user1',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    	dag=dag,
    )

t2 = SimpleHttpOperator(
        task_id='extracting_user2',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
	    dag=dag,
    )

t1 >> t2
