-- The aggregate can only be lower than the fact (hard-deleted articles drop out).
-- Higher means a join fanned out and views are double counted.
select aggregated, fact
from (
    select
        (select sum(view_count) from {{ ref('agg_article_daily') }}) as aggregated,
        (select count(*) from {{ ref('fct_page_views') }}) as fact
)
where aggregated > fact
