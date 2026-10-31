{{ config(
    partition_by={'field': 'date_wib', 'data_type': 'date'},
    cluster_by=['article_id']
) }}

with views as (
    select
        viewed_date_wib as date_wib
        , article_id
        , count(*) as view_count
        , count(distinct session_id) as session_count
        , count(distinct user_id) as logged_in_user_count
    from {{ ref('fct_page_views') }}
    group by 1, 2
)

, comments as (
    select
        commented_date_wib as date_wib
        , article_id
        , countif(not is_deleted) as comment_count
    from {{ ref('fct_comments') }}
    group by 1, 2
)

select
    date_wib
    , article_id
    , a.title
    , a.author_key
    , a.parent_category_name
    , coalesce(v.view_count, 0) as view_count
    , coalesce(v.session_count, 0) as session_count
    , coalesce(v.logged_in_user_count, 0) as logged_in_user_count
    , coalesce(c.comment_count, 0) as comment_count
from views as v
full outer join comments as c
    using (date_wib, article_id)
inner join {{ ref('dim_articles') }} as a
    using (article_id)
