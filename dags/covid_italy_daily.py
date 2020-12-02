from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator


dataset_id = 'airflow-sandbox-296122:airflow'
table_id = 'covid_italy_daily_cases'

dag_args = {
    'owner':'daniel.lee',
    'depends_on_past': False,
    'start_date': datetime(2020,11,19),
    # 'email' : ['dannylee1020@gmail.com'],
    # 'email_on_failure': True,
    'retries' : 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id = 'covid_italy_daily',
    start_date = datetime(2020, 11, 19),
    default_args = dag_args,
    end_date = None,
    schedule_interval = ('0 9 * * *')
)

t1 = BigQueryCheckOperator(task_id = 'check_public_data_exists',
                           bigquery_conn_id = 'bigquery_default',
                           sql = '''
                           select 
                            count(*) > 0
                           from bigquery-public-data.covid19_italy.data_by_region
                           where Date(date) = '{{ ds }}'
                           ''',
                           use_legacy_sql = False)


t2 = BigQueryOperator(task_id = 'load_public_data',
                      use_legacy_sql = False,
                      bigquery_conn_id = 'bigquery_default',
                      create_disposition = 'CREATE_IF_NEEDED',
                      write_disposition = 'WRITE_TRUNCATE',
                      allow_large_results = True,
                      time_partitioning = {
                          'type': 'DAY'
                         },
                      sql = '''
                        select
                            date,
                            sum(total_confirmed_cases) sum_of_daily_cases
                        from bigquery-public-data.covid19_italy.data_by_region
                        where Date(date) = '{{ ds }}'
                        group by 1
                        ''',
                      destination_dataset_table = f"{dataset_id}.{table_id}",
                      dag = dag)



t1 >> t2