"""
dwl-news-transform
------------------
Reads bronze conflict events from S3 (daily JSON), normalizes and filters,
and writes monthly Parquet files to the silver curated bucket.

Layer                : pyarrow-layer:2
                       arn:aws:lambda:us-east-1:472069242258:layer:pyarrow-layer:2
Lambda role          : dwl-news-lambda-role
Secret               : none needed

Input  S3 path : s3://dwl-datapowerchords-raw/news/gemini/
                     ingest_date=<YYYY-MM-DD>/events.json

Output S3 path : s3://dwl-datapowerchords-curated/news/events_parquet/
                     year=<YYYY>/month=<MM>/data.parquet

One Parquet file per month — overwrites on rerun (idempotent).
Snappy compression, strict PyArrow schema.

EventBridge schedule : 0 8 ? * MON *  (08:00 UTC every Monday — 2h after ingest)
Backfill             : invoke with {"backfill": true,
                                    "start_date": "2025-01-01",
                                    "end_date":   "2026-05-02"}
                       Processes month by month, overwrites existing Parquet.

Silver schema
-------------
event_date          date
event_category      string
event_summary       string
severity            int8
date_confidence     string
actors              string   (comma-separated)
location            string
description         string
source_hint         string
url_hash            string
ingest_period       string
"""

import io
import json
import logging
import os
from calendar import monthrange
from datetime import date, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ── logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── constants ─────────────────────────────────────────────────────────────────
BRONZE_BUCKET  = "dwl-datapowerchords-raw"
SILVER_BUCKET  = "dwl-datapowerchords-curated"
BRONZE_PREFIX  = "news/gemini"
SILVER_PREFIX  = "news/events_parquet"
SNS_TOPIC      = "arn:aws:sns:us-east-1:472069242258:dwl-news-alerts"
REGION         = os.environ.get("AWS_REGION", "us-east-1")

# ── strict PyArrow schema ─────────────────────────────────────────────────────
SILVER_SCHEMA = pa.schema([
    pa.field("event_date",      pa.date32()),
    pa.field("event_category",  pa.string()),
    pa.field("event_summary",   pa.string()),
    pa.field("severity",        pa.int8()),
    pa.field("date_confidence", pa.string()),
    pa.field("actors",          pa.string()),
    pa.field("location",        pa.string()),
    pa.field("description",     pa.string()),
    pa.field("source_hint",     pa.string()),
    pa.field("url_hash",        pa.string()),
    pa.field("ingest_period",   pa.string()),
])

# ── clients ───────────────────────────────────────────────────────────────────
_s3_client  = None
_sns_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=REGION)
    return _s3_client


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns", region_name=REGION)
    return _sns_client


def _alert_failures(failed_months: list):
    if not failed_months:
        return
    message  = f"dwl-news-transform — {len(failed_months)} month(s) failed:\n\n"
    message += "\n".join(f"  {m}" for m in failed_months)
    message += "\n\nRerun failed months manually via Lambda test tab."
    _get_sns_client().publish(
        TopicArn = SNS_TOPIC,
        Subject  = f"[DWL] news-transform: {len(failed_months)} month(s) failed",
        Message  = message,
    )
    logger.warning("SNS alert sent for %d failed months.", len(failed_months))


# ── helpers ───────────────────────────────────────────────────────────────────

def _month_ranges(start: date, end: date):
    """Yield (year, month) tuples covering start → end."""
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def _load_bronze_for_month(year: int, month: int) -> list:
    """Load all bronze daily JSON files for a given month. Returns list of raw events."""
    days_in_month = monthrange(year, month)[1]
    all_events    = []

    for day in range(1, days_in_month + 1):
        target_date = date(year, month, day)
        key         = f"{BRONZE_PREFIX}/ingest_date={target_date.isoformat()}/events.json"
        try:
            obj    = _get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
            data   = json.loads(obj["Body"].read())
            events = data.get("events", [])
            all_events.extend(events)
        except _get_s3_client().exceptions.NoSuchKey:
            pass   # empty day — fine
        except Exception:
            logger.warning("Could not load bronze for %s — skipping", target_date)

    logger.info("year=%d month=%02d bronze_events=%d", year, month, len(all_events))
    return all_events


def _normalize_event(e: dict) -> dict | None:
    """
    Normalize a single bronze event into silver schema.
    No filtering — all events are kept regardless of date_confidence,
    category, or severity. Filtering happens at gold.
    Returns None only if event_date is completely unparseable.
    """
    # Parse event_date — only hard reject if date is completely invalid
    try:
        event_date = date.fromisoformat(e.get("event_date", ""))
    except ValueError:
        logger.warning("Dropping event with unparseable date '%s'", e.get("event_date"))
        return None

    # Flatten actors array to comma-separated string
    actors = e.get("actors", [])
    if isinstance(actors, list):
        actors = ", ".join(actors)

    return {
        "event_date":      event_date,
        "event_category":  str(e.get("event_category", "")),
        "event_summary":   str(e.get("event_summary", "")),
        "severity":        int(e.get("severity", 0)),
        "date_confidence": str(e.get("date_confidence", "")),
        "actors":          str(actors),
        "location":        str(e.get("location", "")),
        "description":     str(e.get("description", "")),
        "source_hint":     str(e.get("source_hint", "")),
        "url_hash":        str(e.get("url_hash", "")),
        "ingest_period":   str(e.get("ingest_period", "")),
    }


def _write_parquet(events: list, year: int, month: int) -> str:
    """
    Convert events list to PyArrow table and write as Parquet (Snappy) to S3.
    Returns the S3 key written.
    """
    key = f"{SILVER_PREFIX}/year={year}/month={month:02d}/data.parquet"

    if not events:
        # Write empty Parquet with correct schema so Glue/Athena can still read it
        table = pa.table({field.name: pa.array([], type=field.type)
                          for field in SILVER_SCHEMA}, schema=SILVER_SCHEMA)
    else:
        # Build columns
        table = pa.Table.from_pylist(events, schema=SILVER_SCHEMA)

    # Write to in-memory buffer
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    _get_s3_client().put_object(
        Bucket      = SILVER_BUCKET,
        Key         = key,
        Body        = buf.read(),
        ContentType = "application/octet-stream",
    )
    logger.info(
        "Wrote %d events to s3://%s/%s (%d bytes)",
        len(events), SILVER_BUCKET, key, buf.tell(),
    )
    return key


# ── handler ───────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    Routine run  : event = {}
                   Transforms the current month (re-runs are idempotent).

    Backfill run : event = {"backfill": true,
                             "start_date": "2025-01-01",
                             "end_date":   "2026-05-02"}
    """
    today = date.today()

    if event.get("backfill"):
        raw_start = event.get("start_date", "2025-01-01")
        raw_end   = event.get("end_date",   today.isoformat())
        start_dt  = date.fromisoformat(raw_start)
        end_dt    = date.fromisoformat(raw_end)
        logger.info("Backfill mode: %s -> %s", raw_start, raw_end)
    else:
        # Routine: reprocess current month
        start_dt = date(today.year, today.month, 1)
        end_dt   = today
        logger.info("Routine mode: %s -> %s", start_dt, end_dt)

    results       = []
    failed_months = []

    for year, month in _month_ranges(start_dt, end_dt):
        try:
            bronze_events = _load_bronze_for_month(year, month)

            # Normalize and filter
            silver_events = []
            for e in bronze_events:
                normalized = _normalize_event(e)
                if normalized:
                    silver_events.append(normalized)

            dropped = len(bronze_events) - len(silver_events)
            logger.info(
                "year=%d month=%02d bronze=%d silver=%d dropped=%d",
                year, month, len(bronze_events), len(silver_events), dropped,
            )

            s3_key = _write_parquet(silver_events, year, month)
            results.append({
                "month":        f"{year}-{month:02d}",
                "status":       "ok",
                "bronze_count": len(bronze_events),
                "silver_count": len(silver_events),
                "dropped":      dropped,
                "s3_key":       s3_key,
            })

        except Exception as exc:
            logger.exception("Unexpected error for %d-%02d", year, month)
            failed_months.append(f"{year}-{month:02d}")
            results.append({
                "month":  f"{year}-{month:02d}",
                "status": "error",
                "error":  str(exc),
            })

    ok_count = sum(1 for r in results if r["status"] == "ok")
    logger.info("Done. %d/%d months succeeded.", ok_count, len(results))

    _alert_failures(failed_months)

    return {"statusCode": 200, "results": results}
