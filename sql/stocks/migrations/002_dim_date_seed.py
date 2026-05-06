"""
Seed analytics_gold_dim_date — shared date dimension for the DataPowerChords DWH.

One-shot script: generates 2025-01-01 through 2030-12-31, writes a single
Snappy-compressed Parquet file, and uploads it to the gold zone.

Schema is authoritative per docs/GLUE_CATALOG_REGISTRY.md §2.5 (table 16).

Usage (S3 upload via boto3 — needs SSO credentials):
    pip install pandas pyarrow pandas_market_calendars holidays boto3
    aws sso login --profile <your-sso-profile>
    AWS_PROFILE=<your-sso-profile> python 002_dim_date_seed.py

Usage (local-only — for Console drag-and-drop upload, no AWS creds needed):
    pip install pandas pyarrow pandas_market_calendars holidays
    DIM_DATE_LOCAL_PATH=/tmp/dim_date.parquet python 002_dim_date_seed.py
    # then upload /tmp/dim_date.parquet via the S3 Console

The output key is overwritten on every run (versioning is OFF on the gold
bucket per D15), so re-running is the supported way to refresh if NYSE later
revises its forward calendar.
"""
from __future__ import annotations

import io
import logging
import os
import sys
from pathlib import Path

import holidays
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = "dwl-datapowerchords-gold"
KEY = "analytics/dim_date/dim_date.parquet"
START = "2025-01-01"
END = "2030-12-31"

# When DIM_DATE_LOCAL_PATH is set, write the Parquet to that path on disk
# instead of uploading to S3. Used when the operator prefers to drag-and-drop
# the file into the S3 Console rather than authenticate boto3 via SSO.
LOCAL_PATH = os.environ.get("DIM_DATE_LOCAL_PATH")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dim_date_seed")


def build_dim_date() -> pd.DataFrame:
    dates = pd.date_range(START, END, freq="D")

    nyse = mcal.get_calendar("NYSE")
    trading_days = set(nyse.valid_days(start_date=START, end_date=END).date)

    us_federal = holidays.US(years=range(2025, 2031))

    rows = []
    for ts in dates:
        d = ts.date()
        is_weekend = ts.dayofweek >= 5  # 5=Sat, 6=Sun
        is_trading = d in trading_days
        # NYSE closes on Good Friday (not federal); federal includes Columbus
        # & Veterans Day (NYSE open). Union covers "Federal + NYSE" per spec.
        is_nyse_closure = (not is_trading) and (not is_weekend)
        is_holiday = (d in us_federal) or is_nyse_closure

        rows.append(
            {
                "date_sk": int(ts.strftime("%Y%m%d")),
                "full_date": d,
                "year": ts.year,
                "quarter": int(ts.quarter),
                "month": ts.month,
                "month_name": ts.strftime("%B"),
                "day": ts.day,
                # spec: 0 = Sunday. pandas dayofweek is 0=Mon, so shift.
                "day_of_week": (ts.dayofweek + 1) % 7,
                "day_name": ts.strftime("%A"),
                "week_of_year": int(ts.isocalendar().week),
                "is_weekend": bool(is_weekend),
                "is_us_holiday": bool(is_holiday),
                "is_us_trading_day": bool(is_trading),
                "fiscal_year": ts.year,
            }
        )

    return pd.DataFrame(rows)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    schema = pa.schema(
        [
            ("date_sk", pa.int32()),
            ("full_date", pa.date32()),
            ("year", pa.int16()),
            ("quarter", pa.int16()),
            ("month", pa.int16()),
            ("month_name", pa.string()),
            ("day", pa.int16()),
            ("day_of_week", pa.int16()),
            ("day_name", pa.string()),
            ("week_of_year", pa.int16()),
            ("is_weekend", pa.bool_()),
            ("is_us_holiday", pa.bool_()),
            ("is_us_trading_day", pa.bool_()),
            ("fiscal_year", pa.int16()),
        ]
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def main() -> int:
    log.info("Building dim_date for %s → %s", START, END)
    df = build_dim_date()

    n_rows = len(df)
    n_trading = int(df["is_us_trading_day"].sum())
    n_holidays = int(df["is_us_holiday"].sum())
    log.info(
        "Generated %d rows (%d trading days, %d holidays)",
        n_rows,
        n_trading,
        n_holidays,
    )
    # Sanity: ~252 trading days/year × 6 years ≈ 1512
    expected_trading_lo, expected_trading_hi = 1500, 1525
    if not (expected_trading_lo <= n_trading <= expected_trading_hi):
        log.error(
            "Trading day count %d outside expected [%d, %d] — aborting",
            n_trading,
            expected_trading_lo,
            expected_trading_hi,
        )
        return 1

    body = to_parquet_bytes(df)
    log.info("Parquet size: %.1f KB", len(body) / 1024)

    if LOCAL_PATH:
        out = Path(LOCAL_PATH).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(body)
        log.info("Wrote local file → %s", out)
        log.info("Next: upload to s3://%s/%s via S3 Console", BUCKET, KEY)
        return 0

    import boto3  # local import so the local-only path doesn't require boto3 creds

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=body,
        ContentType="application/octet-stream",
    )
    log.info("Uploaded → s3://%s/%s", BUCKET, KEY)
    return 0


if __name__ == "__main__":
    sys.exit(main())

