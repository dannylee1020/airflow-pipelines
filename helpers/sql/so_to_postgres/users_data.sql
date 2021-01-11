# getting users data from bigquery public data

select
    *
from `bigquery-public-data.stackoverflow.users`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}" 

