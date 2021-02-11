import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.helpers import cross_downstream, chain

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator

# from helpers.scripts import load_data_to_postgres

dag_args = {    
    'owner':'daniel.lee',
    'depends_on_past': False,
    'start_date': datetime(2008, 7, 31),
    'end_date' : datetime(2008, 8, 10),
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id = 'so_data_to_postgres',
    default_args = dag_args,
    catchup = False,
    template_searchpath = ['/Users/dhyungseoklee/Projects/airflow/helpers/sql/so_to_postgres'], 
    schedule_interval = '@daily'
)

# add loop to loop through the tasks

task_starter = DummyOperator(
    task_id = 'task_starter',
    dag = dag
)

# create tables in bigquery
posts_data_task = BigQueryOperator(
    task_id = 'create_posts_data_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'post_data.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_posts_data_{{ ds_nodash }}',
    dag = dag
)

answers_data_task = BigQueryOperator(
    task_id = 'create_answers_data_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'answers_data.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_answers_data_{{ ds_nodash }}',
    dag = dag
)

users_table_task = BigQueryOperator(
    task_id = 'create_users_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'users_table.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_users_table_{{ ds_nodash }}',
    dag = dag
)


# export bigquery tables to gcs
export_posts_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_posts_to_gcs',
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.so_posts_data_{{ ds_nodash }}',    
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/posts/data_{{ ds_nodash }}'],
    print_header = False,
    export_format = 'CSV',
    dag = dag
)


export_answers_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_answers_to_gcs',
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.so_answers_data_{{ ds_nodash }}',
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/post_answers/data_{{ ds_nodash }}'],
    print_header = False,
    export_format = 'CSV',
    dag = dag
)

export_users_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_users_to_gcs',
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.so_users_table_{{ ds_nodash }}',
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/users/data_{{ ds_nodash }}'],   
    print_header = False,
    export_format = 'CSV',
    dag = dag
)
# execute python script
load_data_to_db = BashOperator(
    task_id = 'load_data_to_db',
    bash_command = 'python ${AIRFLOW_HOME}/helpers/scripts/stack_overflow_scripts/load_data_to_postgres.py',
    dag = dag
)


chain(task_starter, [posts_data_task, answers_data_task, users_table_task], [export_posts_to_gcs, export_answers_to_gcs, export_users_to_gcs], load_data_to_db)

