-- Name and email stop here: only a hash moves on to the gold layer.
with latest as (
    {{ latest_per_id(source('bronze', 'users')) }}
)

select
    id as user_id
    , to_hex(sha256(lower(trim(email)))) as email_hash
    , coalesce(g.gender, 'unknown') as gender
    , if(birth_year between 1920 and extract(year from current_date()) - 10, birth_year, null) as birth_year
    , initcap(trim(city)) as city
    , created_at
    , updated_at
    , deleted_at
from latest
left join {{ ref('seed_gender_mapping') }} as g
    on lower(trim(latest.gender)) = g.raw_value
