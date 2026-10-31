select
    article_id
    , category_id
    , is_primary
from {{ source('bronze', 'article_categories') }}
