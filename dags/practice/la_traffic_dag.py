from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
import psycopg2

from helpers.scripts.practice_scripts.get_traffic_data import insert_data, request_data
from helpers.scripts.practice_scripts.get_location import get_location


dag_args = {
    'owner': 'daniel_lee',
    'depends_on_past': False,
    'start_date': datetime(2017,5,1),
    'retry_delay':timedelta(minutes=3)
}

dag = DAG(
    dag_id = 'la_traffic_data',
    default_args = dag_args,
    end_date = datetime(2017,7,1),
    schedule_interval = '@daily'
)

get_data = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = request_data,
        op_kwargs = {'end_point':"https://data.lacounty.gov/resource/uvew-g569.json?countdate={{ ds }}"},
        # provide_context = True,
        dag = dag)

 
# problem with json from previous task
load_data = PythonOperator(
        task_id = 'load_data',
        python_callable = insert_data,
        provide_context = True,
        dag = dag)


get_location_data = PythonOperator(
        task_id = 'get_location',
        python_callable = get_location,
        dag = dag)
        


get_data >> load_data >> get_location_data




