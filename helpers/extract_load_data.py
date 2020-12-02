import requests
from urllib.parse import urlencode
import psycopg2
import psycopg2.extras

from airflow.hooks.postgres_hook import PostgresHook


def request_data(end_point, **kwargs):
    session = requests.Session()
    response = session.get(end_point)
    response.raise_for_status
    data = response.json()

    return data


def insert_data(data, **kwargs):
    connection = psycopg2.connect(
        host = 'localhost',
        user = 'dhyungseoklee',
        database = 'la_traffic',
        password = 'None'
    )

    curr = connection.cursor()

    values = ({
            'location': row['location'],
            'countdate': row['countdate'],
            'dayofweek': row['dayofweek'],
            'totalall':  row['totalall']
    } for row in data)

    psycopg2.extras.execute_batch(curr, '''
        INSERT INTO staging_la_traffic VALUES (
            %(location)s,
            %(count_date)s,
            %(dayofweek)s,
            %(total_count)s);
    ''', values)

    connection.close()







# def insert_data(data, **kwargs):
#     pg_hook = PostgresHook(postgres_conn_id = 'postgres_la_traffic')
    
#     for row in data:
#         values = {
#             'location': row['location'],
#             'countdate': row['countdate'],
#             'dayofweek': row['dayofweek'],
#             'totalall':  row['totalall']
#         } 

#         insert_sql = ''' INSERT INTO staging_la_traffic VALUES(
#                 %(location)s,
#                 %(countdate)s,
#                 %(dayofweek)s,
#                 %(totalall)s ); '''

#         pg_hook.run(insert_sql, parameters = values)









# def iter_data_json(**kwargs):
#     session = requests.Session()
#     while True:
#         response = session.get(kwargs)
#         response.raise_for_status()
#         data = response.json()

#         if not data:
#             break

#         yield from data



# def insert_execute_batch(data, **kwargs):
#     connection = psycopg2.connect(
#         host = 'localhost',
#         database = 'la_traffic',
#         user = 'dhyungseoklee',
#         password = 'None')

#     connection.autocommit = True

#     with connection.cursor() as cursor:
#         create_staging_table(cursor)

#         # iter_data = [{
#         #     'location' : row['location'],
#         #     'count_date': row['countdate'],
#         #     'dayofweek': row['dayofweek'],
#         #     'total_count': row['totalall']
#         # } for row in data]


#         psycopg2.extras.execute_batch(cursor, '''
#             INSERT INTO staging_la_traffic VALUES (
#                 %(location)s,
#                 %(countdate)s,
#                 %(dayofweek)s,
#                 %(totalall)s
#             );
#         ''', data)





    

