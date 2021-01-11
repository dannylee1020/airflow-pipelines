import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator



dag_args = {
    'owner':'daniel.lee',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 3),
    'end_date' : datetime(2016, 1, 6),
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id = 'stack_overflow_tags',
    default_args = dag_args,
    template_searchpath = ['/Users/dhyungseoklee/Projects/airflow/helpers/sql/stack_overflow'],
    catchup = False,
    schedule_interval = '@daily'  
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
    # destination_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}${{ ds_nodash }}",
    destination_dataset_table = 'airflow-sandbox-296122:airflow.stack_overflow_tags_{{ ds_nodash }}',
    dag = dag
)



t3 = BigQueryToCloudStorageOperator(
    task_id = 'export_csv_to_gcs',
    # source_project_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.stack_overflow_tags_{{ ds_nodash }}',
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/stack_overflow_data/data_{{ ds_nodash }}'],
    export_format = 'CSV',
    dag = dag
)



t1 >> t2 >> t3




