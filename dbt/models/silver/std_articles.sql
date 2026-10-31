{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='article_id',
    cluster_by=['article_id']
) }}

with latest as (
    {{ latest_per_id(source('bronze', 'articles')) }}
)

, cleaned as (
    select
        id as article_id
        , regexp_replace(trim(title), r'\s+', ' ') as title
        , trim(regexp_replace(
            replace(replace(regexp_replace(content, r'<[^>]+>', ' '), '&nbsp;', ' '), '&amp;', '&'),
            r'\s+', ' '
        )) as content_text
        , author_id
        , published_at
        , created_at
        , updated_at
        , deleted_at
        , _extracted_at
    from latest
)

select
    article_id
    , if(title = upper(title), initcap(title), title) as title
    , content_text
    , array_length(split(content_text, ' ')) as word_count
    , cast(ceil(array_length(split(content_text, ' ')) / 200) as int64) as reading_minutes
    , author_id
    , case
        when deleted_at is not null then 'deleted'
        when published_at is null then 'draft'
        else 'published'
    end as article_status
    , coalesce(published_at < created_at, false) as is_published_before_created
    , published_at
    , created_at
    , updated_at
    , deleted_at
    , _extracted_at
from cleaned
