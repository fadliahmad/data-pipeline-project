select
    c.id as category_id
    , c.name as category_name
    , c.slug as category_slug
    , coalesce(p.name, c.name) as parent_category_name
    , c.parent_id is null as is_top_level
from {{ source('bronze', 'categories') }} as c
left join {{ source('bronze', 'categories') }} as p
    on c.parent_id = p.id
