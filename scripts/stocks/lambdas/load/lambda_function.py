"""
dwl-stocks-load

Pipeline:
    s3 gold Parquet  →  stocks.fact_prices in dwl-shared-pg

Inputs (read from GOLD_BUCKET):
    stocks/fact_stock_prices/trade_date=<YYYY-MM-DD>/part-0000.parquet

Output:
    rows in stocks.fact_prices, upserted via INSERT … ON CONFLICT (price_sk) DO UPDATE.

Re-running is safe — the W3 transform produces deterministic price_sk values
(price_sk = ticker_sk × 10^9 + date_sk), so re-loading the same partition
overwrites identical rows with identical content. The same idempotency
guarantee applies to the cross-partition backfill loop.

Lambda test events:
    {}                                            daily incremental — load yesterday's partition
    {"trade_date": "2026-04-30"}                  single — load one specific partition
    {"trade_dates": ["2026-04-30", "2026-05-01"]} explicit list — load several specific partitions
    {"backfill": true}                            backfill — list and load every gold partition

Environment variables:
    GOLD_BUCKET       (required) — S3 bucket holding gold Parquet, e.g. dwl-datapowerchords-gold.
    DB_HOST/PORT/NAME/USER (optional) — Postgres connection params; default to the
                       shared dwl-shared-pg endpoint values from the Infrastructure Contract.
    RDS_SECRET_ARN    (optional) — Secrets Manager ARN for the RDS master credentials.
                       Defaults to the auto-generated rds!db-<uuid> secret.
    DB_PASSWORD       (optional) — bypass Secrets Manager and use a literal password.
                       Used by local dev only; the Lambda role doesn't set this.
    LOG_LEVEL         (optional, default INFO).

Layer + role:
    layer  = dwl-stocks-deps:1   (pandas, pyarrow, psycopg2-binary, requests)
    role   = dwl-stocks-lambda-role
"""
from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

GOLD_BUCKET = os.environ["GOLD_BUCKET"]
GOLD_PREFIX = "stocks/fact_stock_prices/"

DB_HOST = os.environ.get("DB_HOST", "dwl-shared-pg.copsauwq0r56.us-east-1.rds.amazonaws.com")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "dwl")
DB_USER = os.environ.get("DB_USER", "dwl_admin")
SECRET_ARN = os.environ.get(
    "RDS_SECRET_ARN",
    "arn:aws:secretsmanager:us-east-1:472069242258:secret:rds!db-9e377778-db31-4627-a0df-da3841e35481-RBkY3b",
)
DB_PASSWORD_OVERRIDE = os.environ.get("DB_PASSWORD")  # for local dev only

s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")


# ─── Partition discovery ───────────────────────────────────────────────────

def list_all_gold_partitions() -> list[str]:
    """Return all trade_date values present under the gold prefix, sorted ascending."""
    paginator = s3.get_paginator("list_objects_v2")
    dates: set[str] = set()
    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix=GOLD_PREFIX):
        for obj in page.get("Contents", []):
            # key shape: stocks/fact_stock_prices/trade_date=YYYY-MM-DD/part-0000.parquet
            for segment in obj["Key"].split("/"):
                if segment.startswith("trade_date="):
                    dates.add(segment.split("=", 1)[1])
                    break
    return sorted(dates)


def read_partition(trade_date_str: str) -> pd.DataFrame | None:
    """Read one gold partition into a DataFrame. Returns None if not found."""
    key = f"{GOLD_PREFIX}trade_date={trade_date_str}/part-0000.parquet"
    try:
        obj = s3.get_object(Bucket=GOLD_BUCKET, Key=key)
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    body = obj["Body"].read()
    return pq.read_table(io.BytesIO(body)).to_pandas()


# ─── DB connection ─────────────────────────────────────────────────────────

def resolve_db_password() -> str:
    if DB_PASSWORD_OVERRIDE:
        return DB_PASSWORD_OVERRIDE
    resp = sm.get_secret_value(SecretId=SECRET_ARN)
    return json.loads(resp["SecretString"])["password"]


def open_connection():
    logger.info("Connecting to %s:%d/%s as %s (sslmode=require)",
                DB_HOST, DB_PORT, DB_NAME, DB_USER)
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER,
        password=resolve_db_password(),
        sslmode="require", connect_timeout=10,
    )


# ─── Insert SQL + row builder ──────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO stocks.fact_prices (
    price_sk, ticker_sk, date_sk, trade_date,
    open, high, low, close, adj_close, volume,
    log_return, ingest_timestamp, source
) VALUES %s
ON CONFLICT (price_sk) DO UPDATE SET
    ticker_sk         = EXCLUDED.ticker_sk,
    date_sk           = EXCLUDED.date_sk,
    trade_date        = EXCLUDED.trade_date,
    open              = EXCLUDED.open,
    high              = EXCLUDED.high,
    low               = EXCLUDED.low,
    close             = EXCLUDED.close,
    adj_close         = EXCLUDED.adj_close,
    volume            = EXCLUDED.volume,
    log_return        = EXCLUDED.log_return,
    ingest_timestamp  = EXCLUDED.ingest_timestamp,
    source            = EXCLUDED.source;
"""


def df_to_rows(df: pd.DataFrame, trade_date_str: str) -> list[tuple]:
    """Convert a gold-zone DataFrame into tuples matching INSERT_SQL column order.

    The gold Parquet body does NOT include `trade_date` (it lives in the S3
    path as the Hive partition key — see decision D20), so we inject the
    trade_date string into every row from the partition we're loading.
    """
    rows: list[tuple] = []
    for _, r in df.iterrows():
        rows.append((
            int(r["price_sk"]),
            int(r["ticker_sk"]),
            int(r["date_sk"]),
            trade_date_str,                       # injected from partition path
            r["open"]      if pd.notna(r["open"])      else None,
            r["high"]      if pd.notna(r["high"])      else None,
            r["low"]       if pd.notna(r["low"])       else None,
            r["close"]     if pd.notna(r["close"])     else None,
            r["adj_close"] if pd.notna(r["adj_close"]) else None,
            int(r["volume"])           if pd.notna(r["volume"])     else None,
            r["log_return"]            if pd.notna(r["log_return"]) else None,
            r["ingest_timestamp"],                # tz-aware datetime
            r["source"],
        ))
    return rows


def load_partition(conn, trade_date_str: str) -> int:
    """Load a single gold partition. Returns rows inserted/updated."""
    df = read_partition(trade_date_str)
    if df is None or df.empty:
        return 0
    rows = df_to_rows(df, trade_date_str)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=500)
    return len(rows)


# ─── Lambda entry ──────────────────────────────────────────────────────────

def lambda_handler(event, context):
    # Mode resolution — first match wins.
    if event.get("backfill"):
        targets = list_all_gold_partitions()
        mode = "backfill"
    elif "trade_dates" in event:
        targets = sorted(set(event["trade_dates"]))
        mode = "explicit-list"
    elif "trade_date" in event:
        targets = [event["trade_date"]]
        mode = "single"
    else:
        # Default: yesterday's partition. EventBridge daily schedule uses this.
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        targets = [yesterday]
        mode = "daily-yesterday"

    logger.info("Mode=%s, targets=%d", mode, len(targets))

    if not targets:
        return {"mode": mode, "partitions_attempted": 0, "rows_loaded": 0}

    rows_loaded = 0
    partitions_loaded = 0
    partitions_missing: list[str] = []

    conn = open_connection()
    try:
        conn.autocommit = False
        for trade_date_str in targets:
            n = load_partition(conn, trade_date_str)
            if n == 0:
                partitions_missing.append(trade_date_str)
            else:
                partitions_loaded += 1
                rows_loaded += n
        conn.commit()
        logger.info("Committed %d rows across %d partitions", rows_loaded, partitions_loaded)
    except Exception:
        conn.rollback()
        logger.exception("Load failed; rolled back")
        raise
    finally:
        conn.close()

    result = {
        "mode": mode,
        "partitions_attempted": len(targets),
        "partitions_loaded": partitions_loaded,
        "partitions_missing": len(partitions_missing),
        "rows_loaded": rows_loaded,
    }
    # Include up to 10 missing examples for quick debugging without bloating the log.
    if partitions_missing:
        result["missing_examples"] = partitions_missing[:10]
    return result
