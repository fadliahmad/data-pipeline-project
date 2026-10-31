{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='page_view_key',
    partition_by={'field': 'viewed_date_wib', 'data_type': 'date'},
    cluster_by=['article_id']
) }}

-- The tracker delivers at least once, so one event can arrive under several ids.
-- An event is identified by what happened, not by the source id.
with events as (
    select
        to_hex(md5(concat(session_id, '|', cast(article_id as string), '|', cast(viewed_at as string)))) as page_view_key
        , *
    from {{ source('bronze', 'page_views') }}
    where true
    {% if is_incremental() %}
        and _extracted_at >= (select timestamp_sub(max(_extracted_at), interval 6 hour) from {{ this }})
    {% endif %}
    qualify row_number() over (partition by page_view_key order by id) = 1
)

select
    page_view_key
    , id as source_id
    , article_id
    , user_id
    , session_id
    , viewed_at
    , date(viewed_at, 'Asia/Jakarta') as viewed_date_wib
    , regexp_contains(lower(user_agent), r'bot|crawler|spider|curl|python-requests') as is_bot
    , case
        when regexp_contains(user_agent, r'^NewsApp') then 'app'
        when regexp_contains(user_agent, r'Mobile|iPhone|Android') then 'mobile_web'
        else 'desktop_web'
    end as device_type
    , if(referrer is null, 'direct', coalesce(r.traffic_channel, 'other')) as traffic_channel
    , _extracted_at
from events
left join {{ ref('seed_referrer_channels') }} as r
    on net.host(events.referrer) = r.referrer_host
