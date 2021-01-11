-- get post data from bigquery public data

select
    *
from `bigquery-public-data.stackoverflow.stackoverflow_posts`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}" 



