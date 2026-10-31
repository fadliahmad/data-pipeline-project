"""Copy source tables from Postgres into BigQuery bronze tables.

The watermark is MAX(_source_change_ts) of the bronze table itself, so it only
advances when a BigQuery load job has committed. There is no separate state to
drift. On an empty table it starts from the oldest source row and walks forward
in CHUNK steps, which makes the initial backfill resumable after a failure.
"""
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipeline.tables import ARTICLE_IDS, TABLES, Table

log = logging.getLogger(__name__)

CHUNK = timedelta(days=30)
# Re-read this much so rows committed late, with an older timestamp, are not lost.
# Silver keeps one row per id, so the overlap causes no duplicates downstream.
OVERLAP = timedelta(minutes=15)
FETCH_SIZE = 20_000
MAX_ROW_DROP = 0.10

ARROW_TYPES = {"INT64": pa.int64(), "STRING": pa.string(), "BOOL": pa.bool_(),
               "TIMESTAMP": pa.timestamp("us", tz="UTC")}


def source_connection():
    conn = psycopg2.connect(os.environ["DATABASE_URL"], options="-c timezone=UTC")
    conn.set_session(readonly=True)
    return conn


def bronze_table_id(client, table):
    return f"{client.project}.{os.environ['BQ_DATASET_BRONZE']}.{table.name}"


def bigquery_client():
    return bigquery.Client(project=os.environ["BQ_PROJECT_ID"], location=os.environ["BQ_LOCATION"])


def arrow_schema(table):
    fields = [(name, ARROW_TYPES[kind]) for name, kind in table.columns]
    if table.change_ts:
        fields.append(("_source_change_ts", ARROW_TYPES["TIMESTAMP"]))
    fields.append(("_extracted_at", ARROW_TYPES["TIMESTAMP"]))
    return pa.schema(fields)


def bigquery_schema(table):
    fields = [bigquery.SchemaField(name, kind) for name, kind in table.columns]
    if table.change_ts:
        fields.append(bigquery.SchemaField("_source_change_ts", "TIMESTAMP"))
    fields.append(bigquery.SchemaField("_extracted_at", "TIMESTAMP"))
    return fields


def select_sql(table):
    schema = os.environ["SOURCE_SCHEMA"]
    # STRING columns are cast because session_id is a uuid in the source.
    columns = [f"{name}::text AS {name}" if kind == "STRING" else name for name, kind in table.columns]
    if table.change_ts:
        columns.append(f"{table.change_ts} AS _source_change_ts")
    sql = f'SELECT {", ".join(columns)} FROM "{schema}"."{table.source_name}"'
    if table.change_ts:
        sql += f" WHERE {table.change_ts} >= %(start)s AND {table.change_ts} < %(end)s"
    return sql


def write_parquet(conn, table, path, params, extracted_at):
    """Stream a query into a Parquet file and return the row count."""
    schema = arrow_schema(table)
    rows = 0
    with conn.cursor(name=f"extract_{table.name}") as cur, pq.ParquetWriter(path, schema) as writer:
        cur.itersize = FETCH_SIZE
        cur.execute(select_sql(table), params)
        while batch := cur.fetchmany(FETCH_SIZE):
            data = {name: [row[i] for row in batch] for i, name in enumerate(schema.names[:-1])}
            data["_extracted_at"] = [extracted_at] * len(batch)
            writer.write_table(pa.table(data, schema=schema))
            rows += len(batch)
    return rows


def load_parquet(client, table, path, write_disposition):
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                    schema=bigquery_schema(table),
                                    write_disposition=write_disposition)
    if table.change_ts:
        config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                             field="_extracted_at")
        config.clustering_fields = ["id"]
    with open(path, "rb") as f:
        client.load_table_from_file(f, bronze_table_id(client, table), job_config=config).result()


def read_watermark(client, table):
    try:
        rows = client.query(f"SELECT MAX(_source_change_ts) AS w FROM `{bronze_table_id(client, table)}`").result()
    except NotFound:
        return None
    return next(iter(rows)).w


def sync_incremental(conn, client, table):
    with conn.cursor() as cur:
        schema = os.environ["SOURCE_SCHEMA"]
        cur.execute(f'SELECT now(), min({table.change_ts}) FROM "{schema}"."{table.source_name}"')
        now, oldest = cur.fetchone()
    conn.commit()
    watermark = read_watermark(client, table)
    start = watermark - OVERLAP if watermark else oldest
    if start is None:
        log.info("%s: source is empty", table.name)
        return 0

    total = 0
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/{table.name}.parquet"
        while start < now:
            end = min(start + CHUNK, now)
            rows = write_parquet(conn, table, path, {"start": start, "end": end}, datetime.now(timezone.utc))
            conn.commit()
            if rows:
                load_parquet(client, table, path, bigquery.WriteDisposition.WRITE_APPEND)
            log.info("%s: %s to %s, %s rows", table.name, start, end, rows)
            total += rows
            start = end
    return total


def check_row_drop(client, table, new_rows):
    try:
        previous = client.get_table(bronze_table_id(client, table)).num_rows
    except NotFound:
        return
    if previous and new_rows < previous * (1 - MAX_ROW_DROP):
        raise ValueError(f"{table.name}: {previous} rows would become {new_rows}. "
                         "Refusing to replace the snapshot; check the source first.")


def sync_snapshot(conn, client, table):
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/{table.name}.parquet"
        rows = write_parquet(conn, table, path, {}, datetime.now(timezone.utc))
        conn.commit()
        if table.guard_row_drop:
            check_row_drop(client, table, rows)
        load_parquet(client, table, path, bigquery.WriteDisposition.WRITE_TRUNCATE)
    log.info("%s: snapshot of %s rows", table.name, rows)
    return rows


def sync(table: Table):
    conn = source_connection()
    try:
        client = bigquery_client()
        if table.change_ts:
            return sync_incremental(conn, client, table)
        return sync_snapshot(conn, client, table)
    finally:
        conn.close()


def sync_table(name):
    return sync(TABLES[name])


def sync_article_ids():
    return sync(ARTICLE_IDS)
