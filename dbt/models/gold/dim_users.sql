select
    user_id
    , email_hash
    , gender
    , birth_year
    , case
        when birth_year is null then 'unknown'
        when extract(year from current_date()) - birth_year < 25 then 'under_25'
        when extract(year from current_date()) - birth_year < 40 then '25_39'
        when extract(year from current_date()) - birth_year < 55 then '40_54'
        else '55_plus'
    end as age_group
    , coalesce(city, 'Unknown') as city
    , deleted_at is not null as is_deleted
    , created_at
from {{ ref('std_users') }}
