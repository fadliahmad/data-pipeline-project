-- dim_articles joins on the primary category, so a second one would duplicate rows.
select article_id, count(*) as primary_count
from {{ ref('std_article_categories') }}
where is_primary
group by article_id
having count(*) > 1
