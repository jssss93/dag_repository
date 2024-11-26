from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json

default_args= {
    #'start_date'=datetime(2021, 1, 1),
    'start_date': days_ago(1),
    'retries': 0,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
}
    
dag = DAG(
        'example_sensor', 
        default_args=default_args, 
        #schedule_interval="@once",
        schedule_interval=timedelta(days=1),
)

file_sensor_a = FileSensor(
    task_id='file_sensor_a',
    fs_conn_id='file_sensor',
    filepath='a.txt',
    dag=dag,
)

read_file_a = BashOperator(
    task_id='read_file_a',
    bash_command='cat /opt/airflow/sensor/a.txt',
    dag=dag,
)

is_api_available = HttpSensor(
    task_id='is_api_available',
    http_conn_id="user_api",
    endpoint="api/",
    dag=dag,
)

extracting_user = SimpleHttpOperator(
    task_id="extracting_user",
    http_conn_id='user_api',
    endpoint='api/',
    method='GET',
    response_filter=lambda response: json.loads(response.content),
    log_response=True,
    dag=dag,
)

file_sensor_a >> read_file_a >> is_api_available >> extracting_user
