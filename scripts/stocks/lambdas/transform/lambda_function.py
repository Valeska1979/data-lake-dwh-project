"""
dwl-stocks-transform

Pipeline:
    bronze yfinance JSON  →  silver Parquet  →  gold Parquet

Inputs (read from RAW_BUCKET):
    stocks/yfinance/symbol=<T>/ingest_date=<YYYY-MM-DD>/daily.json

Outputs:
    silver  →  CURATED_BUCKET  →  stocks/prices/symbol=<T>/year=<YYYY>/month=<MM>/prices.parquet
    gold    →  GOLD_BUCKET     →  stocks/fact_stock_prices/trade_date=<YYYY-MM-DD>/part-0000.parquet

Re-running is safe — every output key is overwritten in place.

How `ingest_date` selection works:
    Bronze contains one partition per ingest run, each carrying the rows
    yfinance returned during that run (a backfill = full history; a routine
    daily run = last ~7 days). Default behaviour is to read the LATEST bronze
    partition for each ticker — correct for daily incremental updates but
    only writes silver/gold for the few days that ingest covered. To populate
    the full silver/gold history on first deploy, pin to the original
    backfill ingest_date (see "seed" event below).

Lambda test events:
    {}                                  daily run — 15 tickers, latest bronze (~7 days written)
    {"tickers": ["XOM"]}                smoke test — one ticker, latest bronze
    {"ingest_date": "2026-05-03"}       seed/replay — pin to a specific bronze snapshot
                                        (used once on first deploy with the backfill date,
                                         then again only for disaster recovery)
"""
import io
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

RAW_BUCKET = os.environ["RAW_BUCKET"]
CURATED_BUCKET = os.environ["CURATED_BUCKET"]
GOLD_BUCKET = os.environ["GOLD_BUCKET"]

# D2 universe — order is the contract for ticker_sk assignment. When
# stocks_gold_dim_ticker (W4 SCD2 dimension) is populated, it must assign
# ticker_sk values matching the (1-based) position in this list.
TICKERS = [
    "XOM", "CVX", "SHEL",
    "COP", "OXY", "DVN",
    "VLO", "MPC",
    "SLB", "HAL", "BKR",
    "FRO",
    "XLE", "USO", "SPY",
]
TICKER_SK_MAP = {symbol: i for i, symbol in enumerate(TICKERS, start=1)}

SOURCE = "yfinance"

s3 = boto3.client("s3")


# ─── Schemas (authoritative per docs/GLUE_CATALOG_REGISTRY.md §2.2 + §2.3) ───
#
# Note: partition-key columns are encoded in the S3 path (Hive style) and are
# NOT stored inside the Parquet body. Glue exposes them as columns at query
# time anyway. Including them in the Parquet schema causes Hive's table
# descriptor to reject the table for "duplicate columns".
#   silver path: symbol=<T>/year=<YYYY>/month=<MM>/prices.parquet
#       partition keys: symbol, year, month
#   gold path:   trade_date=<YYYY-MM-DD>/part-0000.parquet
#       partition keys: trade_date

SILVER_SCHEMA = pa.schema([
    # symbol is a partition key — NOT stored in the Parquet body
    ("trade_date", pa.date32()),
    ("open", pa.decimal128(18, 6)),
    ("high", pa.decimal128(18, 6)),
    ("low", pa.decimal128(18, 6)),
    ("close", pa.decimal128(18, 6)),
    ("adj_close", pa.decimal128(18, 6)),
    ("volume", pa.int64()),
    ("dividend_amount", pa.decimal128(18, 6)),
    ("split_coefficient", pa.decimal128(18, 6)),
    ("log_return", pa.decimal128(18, 8)),
    ("ingest_timestamp", pa.timestamp("us", tz="UTC")),
    ("source", pa.string()),
])

GOLD_SCHEMA = pa.schema([
    ("price_sk", pa.int64()),
    ("ticker_sk", pa.int64()),
    ("date_sk", pa.int32()),
    # trade_date is a partition key — NOT stored in the Parquet body
    ("open", pa.decimal128(18, 6)),
    ("high", pa.decimal128(18, 6)),
    ("low", pa.decimal128(18, 6)),
    ("close", pa.decimal128(18, 6)),
    ("adj_close", pa.decimal128(18, 6)),
    ("volume", pa.int64()),
    ("log_return", pa.decimal128(18, 8)),
    ("ingest_timestamp", pa.timestamp("us", tz="UTC")),
    ("source", pa.string()),
])


# ─── Bronze read helpers ───────────────────────────────────────────────────

def latest_ingest_date(symbol: str) -> str:
    """Return the most recent ingest_date partition for a symbol."""
    prefix = f"stocks/yfinance/symbol={symbol}/"
    paginator = s3.get_paginator("list_objects_v2")
    dates = set()
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            # key shape: stocks/yfinance/symbol=XOM/ingest_date=2026-05-06/daily.json
            parts = obj["Key"].split("/")
            for p in parts:
                if p.startswith("ingest_date="):
                    dates.add(p.split("=", 1)[1])
    if not dates:
        raise RuntimeError(f"No bronze ingest partitions found for symbol={symbol}")
    return max(dates)


def read_bronze(symbol: str, ingest_date: str) -> pd.DataFrame:
    key = f"stocks/yfinance/symbol={symbol}/ingest_date={ingest_date}/daily.json"
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    payload = json.loads(obj["Body"].read())
    rows = payload["rows"]
    if not rows:
        raise RuntimeError(f"Empty bronze rows for {symbol}@{ingest_date}")
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


# ─── Transform: bronze → silver shape ──────────────────────────────────────

def to_decimal(x, scale: int) -> Decimal | None:
    """Float → Decimal via str round-trip. Preserves yfinance's source precision."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return Decimal(str(x)).quantize(Decimal(10) ** -scale)


def build_silver_frame(symbol: str, df_bronze: pd.DataFrame, ingest_ts: datetime) -> pd.DataFrame:
    """Produce a per-symbol silver-shape DataFrame with log_return computed."""
    df = pd.DataFrame()
    df["symbol"] = [symbol] * len(df_bronze)
    df["trade_date"] = df_bronze["trade_date"]

    for col in ("open", "high", "low", "close", "adj_close"):
        df[col] = df_bronze[col].apply(lambda v: to_decimal(v, 6))

    df["volume"] = df_bronze["volume"].astype("Int64")
    df["dividend_amount"] = df_bronze["dividends"].apply(lambda v: to_decimal(v, 6))
    df["split_coefficient"] = df_bronze["stock_splits"].apply(lambda v: to_decimal(v, 6))

    # log_return = ln(adj_close[t] / adj_close[t-1]); first row is NULL.
    adj_close = df_bronze["adj_close"].astype(float)
    log_ret = np.log(adj_close / adj_close.shift(1))
    df["log_return"] = log_ret.apply(lambda v: to_decimal(v, 8) if pd.notna(v) else None)

    df["ingest_timestamp"] = ingest_ts
    df["source"] = SOURCE
    return df


# ─── Parquet writers ───────────────────────────────────────────────────────

def df_to_parquet_bytes(df: pd.DataFrame, schema: pa.Schema) -> bytes:
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def _read_silver_if_exists(key: str) -> pd.DataFrame | None:
    """Return the existing silver DataFrame at the given key, or None if absent."""
    try:
        obj = s3.get_object(Bucket=CURATED_BUCKET, Key=key)
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_silver(symbol: str, df: pd.DataFrame) -> int:
    """Fan out a single-symbol DataFrame into one Parquet file per (year, month).

    For each (symbol, year, month) file, this MERGES the incoming rows with
    any pre-existing rows in that file: rows whose trade_date appears in both
    are replaced by the new version (last write wins on a per-trade_date
    basis), other existing rows are preserved. This makes routine 7-day-
    window runs safe — they extend the monthly file rather than truncating
    it. A full-history seed run (pinned to a backfill ingest_date) still
    behaves like an overwrite because the seed's trade_dates cover the whole
    month.

    The 'symbol' column is dropped before writing — it lives in the S3 path
    as the partition key, not inside the Parquet body.
    """
    if df.empty:
        return 0
    # Drop partition column from Parquet payload (it's encoded in the path)
    df = df.drop(columns=["symbol"])

    # Normalize trade_date to pandas datetime for partition key extraction
    td = pd.to_datetime(df["trade_date"])
    df = df.assign(_year=td.dt.year, _month=td.dt.month)

    written = 0
    for (year, month), new_group in df.groupby(["_year", "_month"]):
        new_group = new_group.drop(columns=["_year", "_month"]).reset_index(drop=True)
        key = (
            f"stocks/prices/"
            f"symbol={symbol}/year={year:04d}/month={month:02d}/prices.parquet"
        )

        existing = _read_silver_if_exists(key)
        if existing is not None and not existing.empty:
            # Normalize trade_date dtype so isin() works regardless of how
            # pyarrow round-tripped the date32 column.
            existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
            new_group["trade_date"] = pd.to_datetime(new_group["trade_date"]).dt.date
            preserved = existing[~existing["trade_date"].isin(new_group["trade_date"])]
            merged = (
                pd.concat([preserved, new_group], ignore_index=True)
                  .sort_values("trade_date")
                  .reset_index(drop=True)
            )
        else:
            merged = new_group

        body = df_to_parquet_bytes(merged, SILVER_SCHEMA)
        s3.put_object(
            Bucket=CURATED_BUCKET, Key=key, Body=body,
            ContentType="application/octet-stream",
        )
        written += 1
        logger.info("silver  → s3://%s/%s (%d rows total, %.1f KB)",
                    CURATED_BUCKET, key, len(merged), len(body) / 1024)
    return written


def build_gold_frame(silver_df: pd.DataFrame, ingest_ts: datetime) -> pd.DataFrame:
    """Project silver columns into the gold fact shape with surrogate keys."""
    if silver_df.empty:
        return silver_df

    df = pd.DataFrame()
    ticker_sk = silver_df["symbol"].map(TICKER_SK_MAP).astype("Int64")
    date_sk = pd.to_datetime(silver_df["trade_date"]).dt.strftime("%Y%m%d").astype("int32")

    # price_sk is a deterministic surrogate: ticker_sk * 10^9 + date_sk.
    # 10^9 dwarfs any YYYYMMDD value so collisions are impossible.
    df["price_sk"] = (ticker_sk.astype("int64") * 1_000_000_000) + date_sk.astype("int64")
    df["ticker_sk"] = ticker_sk
    df["date_sk"] = date_sk
    df["trade_date"] = silver_df["trade_date"]
    for col in ("open", "high", "low", "close", "adj_close"):
        df[col] = silver_df[col]
    df["volume"] = silver_df["volume"]
    df["log_return"] = silver_df["log_return"]
    df["ingest_timestamp"] = ingest_ts
    df["source"] = SOURCE
    return df


def write_gold(gold_df: pd.DataFrame) -> int:
    """One Parquet partition per trade_date (all tickers for that date in one file).

    The 'trade_date' column is dropped before writing — it lives in the S3
    path as the partition key, not inside the Parquet body.
    """
    if gold_df.empty:
        return 0
    written = 0
    for trade_date, group in gold_df.groupby("trade_date"):
        # Drop partition column from Parquet payload (it's encoded in the path)
        group = (
            group.drop(columns=["trade_date"])
                 .sort_values("ticker_sk")
                 .reset_index(drop=True)
        )
        body = df_to_parquet_bytes(group, GOLD_SCHEMA)
        key = (
            f"stocks/fact_stock_prices/"
            f"trade_date={trade_date.isoformat()}/part-0000.parquet"
        )
        s3.put_object(
            Bucket=GOLD_BUCKET, Key=key, Body=body,
            ContentType="application/octet-stream",
        )
        written += 1
    logger.info("gold    → %d trade_date partitions written", written)
    return written


# ─── Lambda entry point ────────────────────────────────────────────────────

def lambda_handler(event, context):
    pinned_date = event.get("ingest_date")
    tickers = event.get("tickers") or TICKERS
    ingest_ts = datetime.now(timezone.utc)

    silver_files = 0
    silver_frames: list[pd.DataFrame] = []
    failed: list[dict] = []

    for symbol in tickers:
        try:
            ingest_date = pinned_date or latest_ingest_date(symbol)
            df_bronze = read_bronze(symbol, ingest_date)
            silver_df = build_silver_frame(symbol, df_bronze, ingest_ts)
            silver_files += write_silver(symbol, silver_df)
            silver_frames.append(silver_df)
        except Exception as e:
            logger.exception("Failed transform for %s", symbol)
            failed.append({"symbol": symbol, "error": str(e)})

    # Cross-symbol gold pass: one file per trade_date covering all symbols.
    gold_partitions = 0
    if silver_frames:
        all_silver = pd.concat(silver_frames, ignore_index=True)
        gold_df = build_gold_frame(all_silver, ingest_ts)
        gold_partitions = write_gold(gold_df)

    result = {
        "tickers_processed": len(tickers) - len(failed),
        "silver_files_written": silver_files,
        "gold_partitions_written": gold_partitions,
        "failed_count": len(failed),
        "failed": failed,
    }

    if failed:
        raise RuntimeError(f"{len(failed)} ticker(s) failed: {failed}")

    return result
