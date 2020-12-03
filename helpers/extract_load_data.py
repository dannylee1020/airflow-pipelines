import requests
from urllib.parse import urlencode
import psycopg2
import psycopg2.extras
import os
import datetime
import json

from airflow.hooks.postgres_hook import PostgresHook


def request_data(end_point, **kwargs):
    session = requests.Session()
    response = session.get(end_point)
    response.raise_for_status
    data = response.json()

    return data
    


def insert_data(**context):

    connection = psycopg2.connect(
        host = 'localhost',
        user = 'dhyungseoklee',
        dbname = 'la_traffic',
        password = None
    )
    curr = connection.cursor()

    # fetch retunred data from previous task using xcom
    data = context['task_instance'].xcom_pull(task_ids = 'get_data_from_api')

    values = [{
        'location': row['location'],
        'countdate': row['countdate'],
        'dayofweek': row['dayofweek'],
        'totalall':  row['totalall']
    } for row in data]
    
    psycopg2.extras.execute_batch(curr, '''
    INSERT INTO staging_la_traffic VALUES(
            %(location)s,
            %(countdate)s,
            %(dayofweek)s,
            %(totalall)s );
    ''', values)

    connection.commit()
    connection.close()





# # storing data from api instead of fetching from xcom
# def request_data(end_point, **kwargs):
#     session = requests.Session()
#     response = session.get(end_point)
#     response.raise_for_status
#     data = response.json()

#     today = datetime.datetime.now().date()
#     filename = f"la_traffic.{str(today)}.json"
#     data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', filename))
#     with open(data_path, 'w') as outputfile:
#         json.dump(data, outputfile)

#     return data
    


# def insert_data():
#     today = datetime.datetime.now().date()
#     filename = f"la_traffic.{str(today)}.json"
#     data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', filename))

#     with open(data_path, 'r') as inputfile:
#         data = json.load(inputfile)


#     connection = psycopg2.connect(
#         host = 'localhost',
#         user = 'dhyungseoklee',
#         database = 'la_traffic',
#         password = 'None'
#     )

#     curr = connection.cursor()

#     values = [{
#             'location': row['location'],
#             'countdate': row['countdate'],
#             'dayofweek': row['dayofweek'],
#             'totalall':  row['totalall']
#     } for row in data]

#     psycopg2.extras.execute_batch(curr, '''
#         INSERT INTO staging_la_traffic VALUES (
#             %(location)s,
#             %(countdate)s,
#             %(dayofweek)s,
#             %(totalall)s);
#     ''', values)

#     connection.commit()
#     connection.close()







    

