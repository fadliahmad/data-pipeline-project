-- Articles loaded to the warehouse that no longer exist in the source.
select
    a.article_id
    , a.title
    , a.author_id
    , a.published_at
    , a.updated_at as last_seen_updated_at
from {{ ref('std_articles') }} as a
left join {{ source('bronze', 'articles_source_ids') }} as s
    on a.article_id = s.id
where s.id is null
