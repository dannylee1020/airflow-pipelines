-- check if the data exists in the table

select
    count(*) > 0
from `bigquery-public-data.stackoverflow.stackoverflow_posts`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}"
limit 1
