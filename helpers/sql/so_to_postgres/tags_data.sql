# getting tags data from bigquery public data

select
    *
from `bigquery-public-data.stackoverflow.tags`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}" 

