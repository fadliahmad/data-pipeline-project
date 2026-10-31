select
    bridge.article_id
    , tag.id as tag_id
    , tag.name as tag_name
from {{ source('bronze', 'article_tags') }} as bridge
inner join {{ source('bronze', 'tags') }} as tag
    on bridge.tag_id = tag.id
