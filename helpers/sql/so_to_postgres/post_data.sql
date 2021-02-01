-- get post data from bigquery public data

select
    id,
    title,
    body,
    accepted_answer_id,
    answer_count,
    comment_count,
    community_owned_date,
    creation_date,
    favorite_count,
    owner_display_name,
    owner_user_id,
    parent_id,
    post_type_id,
    score,
    tags,
    view_count
from `bigquery-public-data.stackoverflow.stackoverflow_posts`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}" 
and owner_user_id not in (
    select distinct 
        p.owner_user_id,
    from `airflow-sandbox-296122.airflow.so_users_table_*` u 
    right join  `airflow-sandbox-296122.airflow.so_posts_data_*` p 
    on u.id = p.owner_user_id
    where u.id is null and p.owner_user_id is not null
)

