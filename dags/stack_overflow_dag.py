import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

project_id = 'airflow-sandbox-296122'
dataset_id = 'airflow'
tags_data_table_id = 'stack_overflow_tags_'
cloud_storage_uri = 'gs://airflow_sandbox_test/data'


dag_args = {
    'owner':'daniel.lee',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'end_date' : datetime(2016, 1, 10),
    'retry_delay': timedelta(minutes=3)
    # 'template_searchpath': 'Users/dhyungseoklee/Projects/airflow/helpers/sql'
    # 'template_searchpath' : '/airflow/helpers/sql'
}

dag = DAG(
    dag_id = 'stack_overflow_tags',
    default_args = dag_args,
    template_searchpath = ['/Users/dhyungseoklee/Projects/airflow/helpers/sql'],
    schedule_interval = '@hourly'
)       


t1 = BigQueryCheckOperator(
    task_id = 'check_if_data_exists',
    bigquery_conn_id = 'bigquery_default',
    sql = 'check_data.sql',
    use_legacy_sql = False,
    dag = dag
)

 

t2 = BigQueryOperator(
    task_id = 'create_tags_data_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'tags_data.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_EMPTY',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    destination_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    dag = dag
)



t3 = BigQueryToCloudStorageOperator(
    task_id = 'export_csv_to_gcs',
    source_project_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    destination_cloud_storage_uris = [cloud_storage_uri],
    export_format = 'csv',
    dag = dag
)



t1 >> t2 >> t3




