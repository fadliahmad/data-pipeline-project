-- Authors whose attributes changed over time, from the SCD Type 2 snapshot.
select
    author_id,
    author_name,
    author_type,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('scd_authors') }}
qualify count(*) over (partition by author_id) > 1
order by author_id, dbt_valid_from
