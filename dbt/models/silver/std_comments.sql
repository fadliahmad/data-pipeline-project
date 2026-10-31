{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='comment_id',
    cluster_by=['article_id']
) }}

with latest as (
    {{ latest_per_id(source('bronze', 'comments')) }}
)

select
    id as comment_id
    , article_id
    , user_id
    , parent_comment_id
    , trim(body) as body
    , parent_comment_id is not null as is_reply
    , created_at
    , updated_at
    , deleted_at
    , _extracted_at
from latest
