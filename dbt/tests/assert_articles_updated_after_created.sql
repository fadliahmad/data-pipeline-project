-- A violation would break the watermark, which relies on these timestamps.
select article_id, created_at, updated_at, deleted_at
from {{ ref('std_articles') }}
where updated_at < created_at
    or deleted_at < created_at
