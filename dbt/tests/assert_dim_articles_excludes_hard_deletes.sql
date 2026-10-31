-- Bonus 2: an article removed from the source must not stay in gold.
select d.article_id
from {{ ref('dim_articles') }} as d
inner join {{ ref('cdc_article_hard_deletes') }} as h
    on d.article_id = h.article_id
