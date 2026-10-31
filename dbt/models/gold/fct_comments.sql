{{ config(
    partition_by={'field': 'commented_date_wib', 'data_type': 'date'},
    cluster_by=['article_id']
) }}

select
    comment_id
    , article_id
    , user_id
    , parent_comment_id
    , is_reply
    , deleted_at is not null as is_deleted
    , created_at
    , date(created_at, 'Asia/Jakarta') as commented_date_wib
from {{ ref('std_comments') }}
