import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor

dag_args = {
    'owner':'daniel.lee',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 3),
    'end_date' : datetime(2016, 1, 6),
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
    destination_dataset_table = 'airflow-sandbox-296122:airflow.so_tags_data_{{ ds_nodash }}',
    dag = dag
)

users_data_task = BigQueryOperator(
    task_id = 'create_users_data_table',
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


# put sensors here until all backfill is complete?
posts_sensor = BigQueryTableSensor(
    task_id = 'posts_sensor',
    project_id = 'airflow-sandbox-296122',
    dataset_id = 'airflow',
    table_id = 'so_posts_data_{{ ds_nodash }}',
    bigquery_conn_id = 'bigquery_default',
    dag = dag
)

users_sensor = BigQueryTableSensor(
    task_id = 'user_sensor',
    project_id = 'airflow-sandbox-296122',
    dataset_id = 'airflow',
    table_id = 'so_users_data_{{ ds_nodash }}',
    bigquery_conn_id = 'bigquery_default'
)

tags_sensor = BigQueryTableSensor(
    task_id = 'tags_sensor',
    project_id = 'airflow-sandbox-296122',  
    dataset_id = 'airflow',
    table_id = 'so_tags_data_{{ ds_nodash }}',
    bigquery_conn_id = 'bigquery_default'
)



# export bigquery tables to gcs
export_posts_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_posts_to_gcs',
    # source_project_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.so_posts_data_{{ ds_nodash }}',    
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/posts/data_{{ ds_nodash }}'],
    print_header = False,
    export_format = 'CSV',
    dag = dag
)


export_tags_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_tags_to_gcs',
    # source_project_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.so_tags_data_{{ ds_nodash }}',
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/tags/data_{{ ds_nodash }}'],
    export_format = 'CSV',
    dag = dag
)

export_users_to_gcs = BigQueryToCloudStorageOperator(
    task_id = 'export_users_to_gcs',
    # source_project_dataset_table = f"{project_id}:{dataset_id}.{tags_data_table_id}",
    source_project_dataset_table = 'airflow-sandbox-296122:airflow.so_users_data_*',
    destination_cloud_storage_uris = ['gs://airflow_sandbox_test/so_to_postgres/users_data/data{{ ds_nodash }}'],
    export_format = 'CSV',
    dag = dag
)



# put gcs sensors here
posts_gcs_sensor = GoogleCloudStorageObjectSensor(
    task_id = 'posts_gcs_sensor',
    bucket = 'gs://airflow_sandbox_test/so_to_postgres/posts',
    object = 'data_{{ ds_nodash }}',
    gcp_conn_id = 'google_cloud_default',
    dag = dag
)





task_starter >> posts_data_task >> posts_sensor >> export_posts_to_gcs >> posts_gcs_sensor
# task_starter >> tags_data_task >> tags_sensor >> export_tags_to_gcs 
# task_starter >> users_data_task >> users_sensor >> export_users_to_gcs

