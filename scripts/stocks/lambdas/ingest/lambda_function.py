import json
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import yfinance as yf

# Lambda's home filesystem is read-only; redirect yfinance's TzCache + CookieCache
# to /tmp (writable ephemeral storage) to silence the noisy startup warnings.
yf.set_tz_cache_location("/tmp/yf-cache")

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = os.environ["BUCKET"]
BACKFILL_START = os.environ.get("BACKFILL_START", "2025-01-01")
ROUTINE_LOOKBACK_DAYS = int(os.environ.get("ROUTINE_LOOKBACK_DAYS", "7"))

TICKERS = [
    "XOM", "CVX", "SHEL",
    "COP", "OXY", "DVN",
    "VLO", "MPC",
    "SLB", "HAL", "BKR",
    "FRO",
    "XLE", "USO", "SPY",
]

COLUMN_MAP = {
    "Date": "date",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
    "Dividends": "dividends",
    "Stock Splits": "stock_splits",
}

s3 = boto3.client("s3")


def fetch_ticker(symbol, start_date):
    t = yf.Ticker(symbol)
    df = t.history(start=start_date, auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"yfinance returned empty DataFrame for {symbol} (start={start_date})")
    df = df.reset_index()
    if "Datetime" in df.columns and "Date" not in df.columns:
        df = df.rename(columns={"Datetime": "Date"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df.rename(columns=COLUMN_MAP)
    keep = [v for v in COLUMN_MAP.values() if v in df.columns]
    return df[keep].to_dict(orient="records")


def write_to_s3(symbol, ingest_date, start_date, rows):
    payload = {
        "meta": {
            "symbol": symbol,
            "ingest_date": ingest_date,
            "start_date": start_date,
            "source": "yfinance",
            "row_count": len(rows),
        },
        "rows": rows,
    }
    key = f"stocks/yfinance/symbol={symbol}/ingest_date={ingest_date}/daily.json"
    body = json.dumps(payload).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    logger.info("Wrote s3://%s/%s (%d rows, %d bytes)", BUCKET, key, len(rows), len(body))
    return key


def lambda_handler(event, context):
    backfill = bool(event.get("backfill"))
    tickers = event.get("tickers") or TICKERS

    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if backfill:
        start_date = BACKFILL_START
    else:
        start_date = (datetime.now(timezone.utc).date() - timedelta(days=ROUTINE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    written, failed = [], []
    for symbol in tickers:
        try:
            rows = fetch_ticker(symbol, start_date)
            key = write_to_s3(symbol, ingest_date, start_date, rows)
            written.append({"symbol": symbol, "key": key, "rows": len(rows)})
        except Exception as e:
            logger.exception("Failed to ingest %s", symbol)
            failed.append({"symbol": symbol, "error": str(e)})

    result = {
        "ingest_date": ingest_date,
        "start_date": start_date,
        "mode": "backfill" if backfill else "routine",
        "written_count": len(written),
        "failed_count": len(failed),
        "written": written,
        "failed": failed,
    }

    if failed:
        raise RuntimeError(f"{len(failed)} ticker(s) failed: {failed}")

    return result
