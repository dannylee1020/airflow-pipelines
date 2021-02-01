# getting tags data from bigquery public data

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
from `bigquery-public-data.stackoverflow.posts_answers` 
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}" 


