"""
Load analytics.dim_date in dwl-shared-pg from the gold Parquet.

Reads s3://dwl-datapowerchords-gold/analytics/dim_date/dim_date.parquet
(or a local Parquet file if DIM_DATE_LOCAL_PATH is set), opens a TLS-only
psycopg2 connection to dwl-shared-pg, and INSERTs all 2,191 rows into
analytics.dim_date with ON CONFLICT (date_sk) DO UPDATE — so re-running is
idempotent and any drift between the gold Parquet and the warehouse rows
is reconciled in one pass.

Cross-team prerequisite: 001_init_analytics.sql must have been applied
first so that analytics.dim_date exists.

Usage (default, fetches DB password from Secrets Manager — needs SSO):
    pip install pandas pyarrow psycopg2-binary boto3
    aws sso login --profile dwl
    AWS_PROFILE=dwl python 003_dim_date_load.py

Usage (creds-free local mode, paste password into env var):
    DB_PASSWORD='...'                    \\
    DIM_DATE_LOCAL_PATH=/tmp/dim_date.parquet  \\
    python 003_dim_date_load.py

The DIM_DATE_LOCAL_PATH and DB_PASSWORD env vars override S3 fetch and
Secrets Manager fetch respectively, so the local-only mode requires zero
AWS credentials. To get the DB password without SSO, copy it from the
RDS master-credentials secret in the AWS Console (Secrets Manager →
the rds!db-<uuid> secret → Retrieve secret value).
"""
from __future__ import annotations

import io
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# RDS connection defaults — match the values in TEAM_INFRASTRUCTURE_CONTRACT.md.
DB_HOST = os.environ.get("DB_HOST", "dwl-shared-pg.copsauwq0r56.us-east-1.rds.amazonaws.com")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "dwl")
DB_USER = os.environ.get("DB_USER", "dwl_admin")
DB_PASSWORD = os.environ.get("DB_PASSWORD")  # if unset, fetched from Secrets Manager

# Master-credentials secret ARN (auto-named by RDS at create time, captured in
# TEAM_INFRASTRUCTURE_CONTRACT.md). The wildcard rds!db-* in each Lambda role's
# inline policy matches this.
SECRET_ARN = os.environ.get(
    "RDS_SECRET_ARN",
    "arn:aws:secretsmanager:us-east-1:472069242258:secret:rds!db-9e377778-db31-4627-a0df-da3841e35481-RBkY3b",
)

# Source Parquet — gold zone, ~33.5 KB. Override with DIM_DATE_LOCAL_PATH for
# creds-free runs against a previously-downloaded copy.
GOLD_BUCKET = "dwl-datapowerchords-gold"
GOLD_KEY = "analytics/dim_date/dim_date.parquet"
LOCAL_PATH = os.environ.get("DIM_DATE_LOCAL_PATH")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dim_date_load")


# ─── Source: read the Parquet (S3 or local) ────────────────────────────────

def read_parquet_bytes() -> bytes:
    if LOCAL_PATH:
        log.info("Reading local Parquet → %s", LOCAL_PATH)
        return Path(LOCAL_PATH).expanduser().resolve().read_bytes()

    import boto3
    log.info("Reading Parquet from s3://%s/%s", GOLD_BUCKET, GOLD_KEY)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=GOLD_BUCKET, Key=GOLD_KEY)
    return obj["Body"].read()


def parquet_to_dataframe(body: bytes) -> pd.DataFrame:
    df = pd.read_parquet(io.BytesIO(body))
    log.info("Loaded %d rows × %d columns from Parquet", len(df), len(df.columns))
    return df


# ─── DB credentials: env var → Secrets Manager fallback ───────────────────

def resolve_db_password() -> str:
    if DB_PASSWORD:
        log.info("Using DB password from DB_PASSWORD env var")
        return DB_PASSWORD

    import boto3
    log.info("Fetching DB password from Secrets Manager → %s", SECRET_ARN)
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=SECRET_ARN)
    secret = json.loads(resp["SecretString"])
    return secret["password"]


# ─── Postgres write ────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO analytics.dim_date (
    date_sk, full_date, year, quarter, month, month_name, day,
    day_of_week, day_name, week_of_year,
    is_weekend, is_us_holiday, is_us_trading_day, fiscal_year
) VALUES %s
ON CONFLICT (date_sk) DO UPDATE SET
    full_date          = EXCLUDED.full_date,
    year               = EXCLUDED.year,
    quarter            = EXCLUDED.quarter,
    month              = EXCLUDED.month,
    month_name         = EXCLUDED.month_name,
    day                = EXCLUDED.day,
    day_of_week        = EXCLUDED.day_of_week,
    day_name           = EXCLUDED.day_name,
    week_of_year       = EXCLUDED.week_of_year,
    is_weekend         = EXCLUDED.is_weekend,
    is_us_holiday      = EXCLUDED.is_us_holiday,
    is_us_trading_day  = EXCLUDED.is_us_trading_day,
    fiscal_year        = EXCLUDED.fiscal_year;
"""


def df_to_tuples(df: pd.DataFrame):
    """Return rows in the column order matched by INSERT_SQL."""
    columns = [
        "date_sk", "full_date", "year", "quarter", "month", "month_name", "day",
        "day_of_week", "day_name", "week_of_year",
        "is_weekend", "is_us_holiday", "is_us_trading_day", "fiscal_year",
    ]
    missing = set(columns) - set(df.columns)
    if missing:
        raise RuntimeError(f"Parquet is missing expected columns: {sorted(missing)}")
    return [tuple(row) for row in df[columns].itertuples(index=False, name=None)]


def load_into_postgres(rows: list[tuple], password: str) -> int:
    log.info("Connecting to %s:%d/%s as %s (sslmode=require)", DB_HOST, DB_PORT, DB_NAME, DB_USER)
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=password,
        sslmode="require",
        connect_timeout=10,
    )
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Sanity check: the table must exist (001_init_analytics.sql ran).
            cur.execute(
                "SELECT to_regclass('analytics.dim_date')::text"
            )
            target = cur.fetchone()[0]
            if target is None:
                raise RuntimeError(
                    "analytics.dim_date does not exist. "
                    "Apply 001_init_analytics.sql first."
                )

            psycopg2.extras.execute_values(
                cur, INSERT_SQL, rows, page_size=500,
            )

            cur.execute("SELECT COUNT(*) FROM analytics.dim_date")
            count = cur.fetchone()[0]
        conn.commit()
        log.info("Committed; analytics.dim_date now has %d rows", count)
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── Entry point ───────────────────────────────────────────────────────────

def main() -> int:
    body = read_parquet_bytes()
    df = parquet_to_dataframe(body)

    # Range sanity matches the seed script's invariant.
    n_rows = len(df)
    n_trading = int(df["is_us_trading_day"].sum())
    if not (1500 <= n_trading <= 1525):
        log.error(
            "Trading-day count %d outside expected [1500, 1525] — refusing to load",
            n_trading,
        )
        return 1
    log.info("Sanity OK: %d total rows, %d trading days", n_rows, n_trading)

    rows = df_to_tuples(df)
    password = resolve_db_password()
    final_count = load_into_postgres(rows, password)

    if final_count != n_rows:
        log.warning(
            "Post-load count %d does not match input %d "
            "(may be expected if rows existed before this run was a strict UPDATE)",
            final_count, n_rows,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
