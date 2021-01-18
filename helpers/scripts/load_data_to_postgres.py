from google.cloud import storage
import os

from helpers.scripts.models import create_db, create_table


bucket_name = 'airflow_sandbox_test'
source_file = 'so_to_postgres/posts/data_20160101'
local_file = 'data_20160101.csv'
destination = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'airflow', 'data', local_filename))

# set google api credential
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/dhyungseoklee/Projects/airflow-sandbox-296122-keyfile.json'


def download_blob(bucket_name, prefix, local_file, destination):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file in bucket.list_blobs(prefix = prefix):
        file_name = file.name.lstrip(prefix)
        destination = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'airflow', 'data', file_name))
        file.download_to_filename(destination)

    print(f"{source_file} downloaded to: {destination}")



if __name__ == '__main__':
    download_blob(bucket_name, source_filename, local_filename)
    engine = create_db()
    create_table(engine)
    conn = engine.raw_connection()
    curr = conn.cursor()
    # print(conn.encoding)
    file_path = '/Users/dhyungseoklee/Projects/airflow/data'
    for file in os.listdir(file_path):
        if file.startswith('data'):
            f = open(os.path.join(file_path, file), encoding = 'utf-8') 
            # bulk loading data 
            cmd = "COPY stack_overflow_posts FROM STDIN WITH (FORMAT CSV)"
            curr.copy_expert(cmd, f)
            conn.commit()
        else:
            continue
