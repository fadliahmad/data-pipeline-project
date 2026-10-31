select
    author_id as author_key
    , author_name
    , email
    , author_type
    , deleted_at is not null as is_deleted
    , created_at
    , updated_at
from {{ ref('std_authors') }}
where author_id = canonical_author_id

union all

-- Target for articles whose author row no longer exists.
select
    -1
    , 'Unknown'
    , cast(null as string)
    , 'unknown'
    , false
    , cast(null as timestamp)
    , cast(null as timestamp)
