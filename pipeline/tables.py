"""Source tables and how each one syncs into the bronze layer."""
from dataclasses import dataclass

SOFT_DELETE_CHANGE_TS = "GREATEST(updated_at, deleted_at)"
AUDIT_COLUMNS = (("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"), ("deleted_at", "TIMESTAMP"))


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple
    # SQL expression for "last changed". None means full snapshot on every run.
    change_ts: str | None = None
    # Source table, when it differs from name.
    source: str | None = None
    # Fail instead of replacing the snapshot if the row count falls too far.
    guard_row_drop: bool = False

    @property
    def source_name(self):
        return self.source or self.name


TABLES = {t.name: t for t in (
    Table("articles", (
        ("id", "INT64"), ("title", "STRING"), ("content", "STRING"), ("published_at", "TIMESTAMP"),
        ("author_id", "INT64"), *AUDIT_COLUMNS), change_ts=SOFT_DELETE_CHANGE_TS),
    Table("authors", (
        ("id", "INT64"), ("name", "STRING"), ("email", "STRING"), ("author_type", "STRING"),
        *AUDIT_COLUMNS), change_ts=SOFT_DELETE_CHANGE_TS),
    Table("users", (
        ("id", "INT64"), ("name", "STRING"), ("email", "STRING"), ("gender", "STRING"),
        ("birth_year", "INT64"), ("city", "STRING"), *AUDIT_COLUMNS), change_ts=SOFT_DELETE_CHANGE_TS),
    Table("comments", (
        ("id", "INT64"), ("article_id", "INT64"), ("user_id", "INT64"), ("parent_comment_id", "INT64"),
        ("body", "STRING"), *AUDIT_COLUMNS), change_ts=SOFT_DELETE_CHANGE_TS),
    Table("page_views", (
        ("id", "INT64"), ("article_id", "INT64"), ("user_id", "INT64"), ("session_id", "STRING"),
        ("viewed_at", "TIMESTAMP"), ("user_agent", "STRING"), ("referrer", "STRING")), change_ts="viewed_at"),
    # No delete marker and few rows, so a snapshot also picks up hard deletes.
    Table("categories", (
        ("id", "INT64"), ("name", "STRING"), ("slug", "STRING"), ("parent_id", "INT64"),
        ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"))),
    Table("tags", (("id", "INT64"), ("name", "STRING"), ("created_at", "TIMESTAMP"))),
    Table("article_categories", (
        ("article_id", "INT64"), ("category_id", "INT64"), ("is_primary", "BOOL"), ("created_at", "TIMESTAMP"))),
    Table("article_tags", (("article_id", "INT64"), ("tag_id", "INT64"), ("created_at", "TIMESTAMP"))),
)}

# Must run after the articles sync: an id loaded to bronze is then always in this
# list unless it really was deleted. In the other order, an article created
# between the two steps would look hard-deleted.
ARTICLE_IDS = Table("articles_source_ids", (("id", "INT64"),), source="articles", guard_row_drop=True)
