"""
dwl-oil-load
-------------
Reads gold Parquet files from S3 and upserts into
oil.fact_prices in RDS (PostgreSQL).

Idempotent:
    ON CONFLICT (price_sk) DO UPDATE

Input:
    s3://dwl-datapowerchords-gold/oil/fact_oil_prices/
        trade_date=<YYYY-MM-DD>/part-0000.parquet

Environment variables:
    GOLD_BUCKET
    RDS_SECRET_ARN
    LOG_LEVEL
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import date, timedelta

import boto3
import pyarrow.parquet as pq
import psycopg2

# ──────────────────────────────────────────────────────────────────────────────
# logging
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# ──────────────────────────────────────────────────────────────────────────────
# constants
# ──────────────────────────────────────────────────────────────────────────────

GOLD_BUCKET = os.environ["GOLD_BUCKET"]

RDS_SECRET_ARN = os.environ["RDS_SECRET_ARN"]

GOLD_PREFIX = "oil/fact_oil_prices"

RDS_HOST = "dwl-shared-pg.copsauwq0r56.us-east-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DB = "dwl"

REGION = os.environ.get("AWS_REGION", "us-east-1")

# ──────────────────────────────────────────────────────────────────────────────
# clients
# ──────────────────────────────────────────────────────────────────────────────

s3 = boto3.client("s3", region_name=REGION)
sm = boto3.client("secretsmanager", region_name=REGION)

# ──────────────────────────────────────────────────────────────────────────────
# secrets / db
# ──────────────────────────────────────────────────────────────────────────────

_cached_creds = None


def get_rds_creds():
    global _cached_creds

    if _cached_creds is None:
        response = sm.get_secret_value(SecretId=RDS_SECRET_ARN)
        _cached_creds = json.loads(response["SecretString"])

    return _cached_creds


def get_connection():
    creds = get_rds_creds()

    return psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DB,
        user=creds["username"],
        password=creds["password"],
        sslmode="require",
    )

# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────


def load_gold_parquet(trade_date: date):
    key = (
        f"{GOLD_PREFIX}/"
        f"trade_date={trade_date.isoformat()}/"
        f"part-0000.parquet"
    )

    try:
        obj = s3.get_object(Bucket=GOLD_BUCKET, Key=key)

        buffer = io.BytesIO(obj["Body"].read())

        table = pq.read_table(buffer)

        return table.to_pylist()

    except s3.exceptions.NoSuchKey:
        logger.info("No gold file found for %s", trade_date)
        return None

    except Exception:
        logger.exception("Failed loading parquet for %s", trade_date)
        return None


def get_date_sk(conn, trade_date):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT date_sk
            FROM analytics.dim_date
            WHERE full_date = %s
            """,
            (trade_date,)
        )

        result = cur.fetchone()

        if result:
            return result[0]

        return None


def upsert_rows(conn, rows, trade_date):
    date_sk = get_date_sk(conn, trade_date)

    if date_sk is None:
        logger.warning("No date_sk found for %s", trade_date)
        return 0

    sql = """
    INSERT INTO oil.fact_prices (
        price_sk,
        date_sk,
        trade_date,
        price,
        oil_type,
        ingest_timestamp,
        source
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (price_sk)
    DO UPDATE SET
        date_sk = EXCLUDED.date_sk,
        trade_date = EXCLUDED.trade_date,
        price = EXCLUDED.price,
        oil_type = EXCLUDED.oil_type,
        ingest_timestamp = EXCLUDED.ingest_timestamp,
        source = EXCLUDED.source
    """

    inserted = 0

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                sql,
                (
                    int(row["price_sk"]),
                    int(date_sk),
                    trade_date,
                    row["price"],
                    row["oil_type"],
                    row["ingest_timestamp"],
                    row["source"],
                )
            )

            inserted += 1

    conn.commit()

    return inserted


def daterange(start_date, end_date):
    current = start_date

    while current <= end_date:
        yield current
        current += timedelta(days=1)

# ──────────────────────────────────────────────────────────────────────────────
# lambda handler
# ──────────────────────────────────────────────────────────────────────────────


def lambda_handler(event, context):

    today = date.today()

    # backfill mode
    if event.get("backfill"):

        start_date = date.fromisoformat(
            event.get("start_date", "2025-01-01")
        )

        end_date = date.fromisoformat(
            event.get("end_date", today.isoformat())
        )

        logger.info(
            "Backfill mode: %s -> %s",
            start_date,
            end_date,
        )

    # single date
    elif "trade_date" in event:

        start_date = date.fromisoformat(event["trade_date"])
        end_date = start_date

        logger.info(
            "Single-date mode: %s",
            start_date,
        )

    # default daily mode
    else:

        end_date = today - timedelta(days=1)
        start_date = end_date

        logger.info(
            "Daily mode: %s",
            start_date,
        )

    results = []

    conn = get_connection()

    try:

        for target_date in daterange(start_date, end_date):

            rows = load_gold_parquet(target_date)

            if rows is None:

                results.append({
                    "date": target_date.isoformat(),
                    "status": "missing"
                })

                continue

            inserted = upsert_rows(
                conn,
                rows,
                target_date,
            )

            logger.info(
                "Loaded %s rows for %s",
                inserted,
                target_date,
            )

            results.append({
                "date": target_date.isoformat(),
                "rows_loaded": inserted,
                "status": "ok"
            })

    finally:
        conn.close()

    return {
        "statusCode": 200,
        "results": results
    }