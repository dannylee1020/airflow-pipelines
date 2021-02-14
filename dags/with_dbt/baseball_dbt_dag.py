import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator

dag_args = {    
    'owner':'daniel.lee',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 10),
    'end_date' : datetime(2021, 2, 15),
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id = 'baseball_dbt',
    default_args = dag_args,
    catchup = False,
    template_searchpath = ['/Users/dhyungseoklee/Projects/airflow/helpers/sql/baseball'], 
    schedule_interval = '@hourly'
) as dag:

    t1 = BigQueryOperator(
        task_id = 'public_data_to_stg',
        bigquery_conn_id = 'bigquery_default',
        sql = 'get_public_data.sql',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        use_legacy_sql = False,
        # time_partitioning = {
        #     'type' : 'DAY'
        # },
        destination_dataset_table = 'airflow-sandbox-296122:airflow_dbt.games_post_wide'
    ) 

# running dbt with Docker container
    t2 = DockerOperator(
        task_id = 'run_docker_dbt',
        image = 'dannylee1020/baseball_dbt',
        docker_url = 'unix://var/run/docker.sock',  
        volumes = ['/Users/dhyungseoklee/.config/gcloud:/root/.config/gcloud'],
        api_version = 'auto'
    )

# running dbt with BashOperator
    # t2 = BashOperator(
    #     task_id = 'dbt_docs',
    #     bash_command = 'cd /Users/dhyungseoklee/Projects/dbt/baseball_dbt && dbt docs generate'
    # )

    # t3 = BashOperator(
    #     task_id = 'dbt_test',
    #     bash_command = 'cd /Users/dhyungseoklee/Projects/dbt/baseball_dbt && dbt test'
    # )

    # t4 = BashOperator(
    #     task_id = 'dbt_run',
    #     bash_command = 'cd /Users/dhyungseoklee/Projects/dbt/baseball_dbt && dbt run'
    # )




# t1 >> t2 >> t3 >> t4
t1 >> t2



