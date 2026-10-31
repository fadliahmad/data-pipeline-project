with published as (
    select
        date_trunc(published_date_wib, month) as month_wib
        , author_key
        , count(*) as articles_published
        , round(avg(word_count)) as avg_word_count
    from {{ ref('dim_articles') }}
    where published_date_wib is not null and article_status = 'published'
    group by 1, 2
)

, engagement as (
    select
        date_trunc(date_wib, month) as month_wib
        , author_key
        , sum(view_count) as view_count
        , sum(comment_count) as comment_count
    from {{ ref('agg_article_daily') }}
    group by 1, 2
)

select
    month_wib
    , author_key
    , d.author_name
    , d.author_type
    , coalesce(p.articles_published, 0) as articles_published
    , p.avg_word_count
    , coalesce(e.view_count, 0) as view_count
    , coalesce(e.comment_count, 0) as comment_count
from published as p
full outer join engagement as e
    using (month_wib, author_key)
inner join {{ ref('dim_authors') }} as d
    using (author_key)
