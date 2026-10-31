-- Most-read published articles in the 30 days before the latest data.
-- The dummy data ends in 2025, so the window is not based on today's date.
with latest as (
    select max(date_wib) as max_date
    from {{ ref('agg_article_daily') }}
)

select
    d.article_id,
    d.title,
    d.parent_category_name,
    sum(d.view_count) as view_count,
    sum(d.session_count) as session_count,
    sum(d.comment_count) as comment_count
from {{ ref('agg_article_daily') }} as d
cross join latest
where d.date_wib > date_sub(latest.max_date, interval 30 day)
group by 1, 2, 3
order by view_count desc
limit 20
