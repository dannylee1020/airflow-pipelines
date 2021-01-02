-- get tags data

with overall_data as(
select 
  format_timestamp('%Y-%m-%d', creation_date) as created_date,
  coalesce(format_timestamp('%Y-%m-%d', last_edit_date), 'unspecified') as edited_date,
  case
    when tags = '' then 'unspecified'
    else tags
  end as tags
from `bigquery-public-data.stackoverflow.stackoverflow_posts`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}"
)

select
    created_date,
    tags,
    count(*)
from overall_data
where tags != 'unspecified' 
group by 1,2

