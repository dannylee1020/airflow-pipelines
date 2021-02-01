# getting users data from bigquery public data
/*
duplicate users could exist on different dates in posts and post_answers
-> start date from the beginning
-> remove last edit fields 
*/


select
  *
from `bigquery-public-data.stackoverflow.users`
where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}"




-- select   
--   *
-- from `bigquery-public-data.stackoverflow.users`
-- where id in (
--   select
--     owner_user_id
--   from `bigquery-public-data.stackoverflow.stackoverflow_posts`
--   where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}"
  
--   union distinct
  
--   select  
--     owner_user_id
--   from `bigquery-public-data.stackoverflow.posts_answers`
--   where format_timestamp('%Y-%m-%d', creation_date) = "{{ ds }}"
-- )


