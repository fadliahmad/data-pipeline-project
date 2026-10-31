{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'viewed_date_wib', 'data_type': 'date'},
    cluster_by=['article_id']
) }}

-- Human traffic only. Bots stay in std_page_views for anyone who needs them.
select
    page_view_key
    , article_id
    , user_id
    , session_id
    , viewed_at
    , viewed_date_wib
    , device_type
    , traffic_channel
    , row_number() over (partition by session_id order by viewed_at, page_view_key) as view_order_in_session
from {{ ref('std_page_views') }}
where not is_bot
{% if is_incremental() %}
    and viewed_date_wib >= date_sub((select max(viewed_date_wib) from {{ this }}), interval 3 day)
{% endif %}
