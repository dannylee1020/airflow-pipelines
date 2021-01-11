import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

dag_args = {
    'owner':'daniel.lee',
    'depends_on_past': False,
    # 'start_date': datetime(2016, 1, 3),
    # 'end_date' : datetime(2016, 1, 6),
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id = 'so_data_to_postgres',
    default_args = dag_args,
    catchup = False,
    template_searchpath = ['/Users/dhyungseoklee/Projects/airflow/helpers/sql/so_to_postgres'],
    schedule_interval = '@daily'
)

dummy_opreator = DummyOperator(
    task_id = 'task_starter',
    dag = dag
)

posts_data_task = BigQueryOperator(
    task_id = 'create_posts_data_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'post_data.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_EMPTY',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    # destination_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}${{ ds_nodash }}",
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_posts_data_{{ ds_nodash }}',
    dag = dag
)

tags_data_task = BigQueryOperator(
    task_id = 'create_posts_data_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'tags_data.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_EMPTY',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    # destination_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}${{ ds_nodash }}",
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_tags_data_{{ ds_nodash }}',
    dag = dag
)

users_data_task = BigQueryOperator(
    task_id = 'create_posts_data_table',
    bigquery_conn_id = 'bigquery_default',
    sql = 'users_data.sql',
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_EMPTY',
    use_legacy_sql = False,
    time_partitioning = {
        'type':'DAY'
    },
    # destination_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}${{ ds_nodash }}",
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_users_data_{{ ds_nodash }}',
    dag = dag
)


export_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_csv_to_gcs',
    # source_project_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    source_project_dataset_table = 'airflow-sandbox-296122:airflow. {{ ds_nodash }}',
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/data_{{ ds_nodash }}'],
    export_format = 'CSV',
    dag = dag
)








dummy_operator >> posts_data_task
dummy_operator >> tags_data_task
dummy_operator >> users_data_task

