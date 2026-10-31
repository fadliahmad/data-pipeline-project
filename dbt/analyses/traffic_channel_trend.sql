-- Share of human views per traffic channel, by year.
select
    extract(year from viewed_date_wib) as year,
    traffic_channel,
    count(*) as view_count,
    round(100 * count(*) / sum(count(*)) over (partition by extract(year from viewed_date_wib)), 1) as pct_of_year
from {{ ref('fct_page_views') }}
group by 1, 2
order by 1, 3 desc
