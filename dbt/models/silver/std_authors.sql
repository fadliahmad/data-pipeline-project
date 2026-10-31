with latest as (
    {{ latest_per_id(source('bronze', 'authors')) }}
)

, cleaned as (
    select
        id as author_id
        , initcap(regexp_replace(trim(name), r'\s+', ' ')) as author_name
        , lower(trim(email)) as email
        , author_type
        , created_at
        , updated_at
        , deleted_at
    from latest
)

select
    *
    -- The same person registered twice shares an email. Keep the oldest id.
    , if(email is null, author_id, min(author_id) over (partition by email)) as canonical_author_id
from cleaned
