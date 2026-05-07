"""
006_dim_ticker_export.py

Read stocks.dim_ticker from dwl-shared-pg and write a Parquet snapshot to
the gold zone for Glue Catalog symmetry.

Mirrors the convention from 003_dim_date_load.py: DB_PASSWORD env var, S3
upload via boto3, optional DIM_TICKER_LOCAL_PATH for creds-free local-only
mode followed by S3 Console drag-and-drop.

Why bother having a Parquet copy of dim_ticker when the canonical version
already lives in stocks.dim_ticker (RDS):
  - Catalog registry row 04 (`stocks_gold_dim_ticker`) requires it.
  - Q3 sector grouping in Tableau-on-Athena dashboards needs to JOIN to a
    catalogued ticker dimension that lives in the same data plane as the
    gold facts.
  - Section 2 (Data Lake) rubric line "data catalog with tags, owners,
    descriptions" benefits from having all 4 stocks-domain Glue tables
    (bronze, silver, gold-fact, gold-dim) registered, not just 3.

Idempotent: re-running overwrites the Parquet at the same key. Glue gold
versioning is OFF (D15) so the previous version is gone — that's fine,
the source of truth is the RDS table, not this snapshot.

Usage (default, S3 upload via boto3 — needs SSO / Lambda role):
    pip install pandas pyarrow psycopg2-binary boto3
    DB_PASSWORD='...' python 006_dim_ticker_export.py

Usage (creds-free local mode, drag-and-drop via S3 Console afterwards):
    DB_PASSWORD='...' DIM_TICKER_LOCAL_PATH=/tmp/dim_ticker.parquet \\
        python 006_dim_ticker_export.py
"""
from __future__ import annotations

import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

DB_HOST = os.environ.get("DB_HOST", "dwl-shared-pg.copsauwq0r56.us-east-1.rds.amazonaws.com")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "dwl")
DB_USER = os.environ.get("DB_USER", "dwl_admin")
DB_PASSWORD = os.environ["DB_PASSWORD"]   # required; set from Secrets Manager fetch

GOLD_BUCKET = "dwl-datapowerchords-gold"
GOLD_KEY = "stocks/dim_ticker/dim_ticker.parquet"
LOCAL_PATH = os.environ.get("DIM_TICKER_LOCAL_PATH")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dim_ticker_export")

# Schema mirrors docs/GLUE_CATALOG_REGISTRY.md §2.4 column-for-column.
SCHEMA = pa.schema([
    ("ticker_sk",    pa.int64()),
    ("ticker",       pa.string()),
    ("company_name", pa.string()),
    ("sector",       pa.string()),
    ("subsector",    pa.string()),
    ("country",      pa.string()),
    ("exchange",     pa.string()),
    ("is_benchmark", pa.bool_()),
    ("valid_from",   pa.timestamp("us", tz="UTC")),
    ("valid_to",     pa.timestamp("us", tz="UTC")),
])


def fetch_dim_ticker() -> pd.DataFrame:
    log.info("Connecting to %s:%d/%s as %s (sslmode=require)",
             DB_HOST, DB_PORT, DB_NAME, DB_USER)
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, sslmode="require", connect_timeout=10,
    )
    try:
        sql = """
            SELECT ticker_sk, ticker, company_name, sector, subsector,
                   country, exchange, is_benchmark, valid_from, valid_to
            FROM   stocks.dim_ticker
            ORDER  BY ticker_sk
        """
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    log.info("Fetched %d rows from stocks.dim_ticker", len(df))
    return df


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def main() -> int:
    df = fetch_dim_ticker()
    if len(df) != 15:
        log.error("Expected 15 dim_ticker rows, got %d — refusing to export", len(df))
        return 1

    body = to_parquet_bytes(df)
    log.info("Parquet size: %.1f KB", len(body) / 1024)

    if LOCAL_PATH:
        out = Path(LOCAL_PATH).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(body)
        log.info("Wrote local file → %s", out)
        log.info("Next: upload to s3://%s/%s via S3 Console", GOLD_BUCKET, GOLD_KEY)
        return 0

    import boto3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=GOLD_BUCKET, Key=GOLD_KEY, Body=body,
        ContentType="application/octet-stream",
    )
    log.info("Uploaded → s3://%s/%s", GOLD_BUCKET, GOLD_KEY)
    return 0


if __name__ == "__main__":
    sys.exit(main())
