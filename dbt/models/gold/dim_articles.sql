{{ config(cluster_by=['article_id']) }}

-- Inner join on the source id list: this is what removes hard-deleted articles.
-- Soft-deleted articles stay, with article_status = 'deleted'.
with tags as (
    select article_id, array_agg(tag_name order by tag_name) as tags
    from {{ ref('std_article_tags') }}
    group by article_id
)

, primary_category as (
    select article_id, category_id
    from {{ ref('std_article_categories') }}
    where is_primary
)

select
    a.article_id
    , a.title
    , a.word_count
    , a.reading_minutes
    , a.article_status
    , coalesce(au.canonical_author_id, -1) as author_key
    , au.author_id is null as is_author_missing
    , c.category_name
    , c.parent_category_name
    , coalesce(t.tags, []) as tags
    , a.is_published_before_created
    , a.published_at
    , date(a.published_at, 'Asia/Jakarta') as published_date_wib
    , timestamp_diff(a.published_at, a.created_at, minute) as minutes_to_publish
    , a.created_at
    , a.updated_at
    , a.deleted_at
from {{ ref('std_articles') }} as a
inner join {{ source('bronze', 'articles_source_ids') }} as s
    on a.article_id = s.id
left join {{ ref('std_authors') }} as au
    on a.author_id = au.author_id
left join primary_category as pc
    on a.article_id = pc.article_id
left join {{ ref('dim_categories') }} as c
    on pc.category_id = c.category_id
left join tags as t
    on a.article_id = t.article_id
