select
    category_id
    , category_name
    , category_slug
    , parent_category_name
from {{ ref('std_categories') }}
where not is_top_level
