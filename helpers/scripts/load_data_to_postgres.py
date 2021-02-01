from google.cloud import storage
import os
from helpers.scripts.models import create_db, create_table


bucket_name = 'airflow_sandbox_test'
posts_prefix = 'so_to_postgres/posts/'
users_prefix = 'so_to_postgres/users/'
post_answers_prefix = 'so_to_postgres/post_answers'

# set google api credential
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/dhyungseoklee/Projects/airflow-sandbox-296122-keyfile.json'


def download_blob(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file in bucket.list_blobs(prefix = prefix):
        file_name = str(file.name.lstrip(prefix)) + '.csv'
        if prefix == posts_prefix:
            destination = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'posts_data', file_name))
        elif prefix == users_prefix:
            destination = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'users_data', file_name))
        else:
            destination = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'answers_data', file_name))
        file.download_to_filename(destination)


if __name__ == '__main__':
    download_blob(bucket_name, posts_prefix)
    download_blob(bucket_name, users_prefix)
    download_blob(bucket_name, post_answers_prefix)
    engine = create_db()
    create_table(engine)
    conn = engine.raw_connection()
    curr = conn.cursor()

    file_path = '/Users/dhyungseoklee/Projects/airflow/data'
    for dir in sorted(os.listdir(file_path), reverse = True):
        if not dir.startswith('.'):
            for file in os.listdir(file_path + '/' + dir):
                if file.startswith('data'):
                    f = open(os.path.join(os.path.join(file_path, dir), file), encoding = 'utf-8') 
                    # bulk loading data 
                    if dir.startswith('users'):
                        cmd = "COPY users FROM STDIN WITH (FORMAT CSV)"
                    elif dir.startswith('posts'):
                        cmd = "COPY posts FROM STDIN WITH (FORMAT CSV)"
                    else:
                        # cmd = "COPY posts_answers FROM STDIN WITH (FORMAT CSV)"
                        continue
                    curr.copy_expert(cmd, f)
                    conn.commit()
                else:
                    continue
