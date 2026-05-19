"""
dwl-news-transform
------------------
Reads bronze conflict events from S3 (daily JSON), writes:
  1. Silver — all events, normalized, monthly Parquet (no filtering)
  2. Gold   — daily aggregates, filtered (no approximate, no severity 1)
              includes category_sk and severity_sk for star schema FK support

Layer                : dwl-stocks-deps
Lambda role          : dwl-news-lambda-role
Runtime              : Python 3.11

Input  : s3://dwl-datapowerchords-raw/news/gemini/
             event_date=<YYYY-MM-DD>/events.json

Silver : s3://dwl-datapowerchords-curated/news/events_parquet/
             year=<YYYY>/month=<MM>/data.parquet

Gold   : s3://dwl-datapowerchords-gold/news/fact_conflict_daily/
             event_date=<YYYY-MM-DD>/data.parquet

EventBridge schedule : 0 8 ? * MON *  (08:00 UTC every Monday)
Backfill             : invoke with {"backfill": true,
                                    "start_date": "2025-01-01",
                                    "end_date":   "2026-05-17"}
"""

import io
import json
import logging
import os
from calendar import monthrange
from collections import defaultdict
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
GOLD_BUCKET    = "dwl-datapowerchords-gold"
BRONZE_PREFIX  = "news/gemini"
SILVER_PREFIX  = "news/events_parquet"
GOLD_PREFIX    = "news/fact_conflict_daily"
SNS_TOPIC      = "arn:aws:sns:us-east-1:472069242258:dwl-news-alerts"
REGION         = os.environ.get("AWS_REGION", "us-east-1")

# Maps event_category code -> category_sk in news_events.dim_event_category
CATEGORY_SK_MAP = {
    "military_conflict":  1,
    "sanctions":          2,
    "security_incidents": 3,
    "nuclear_diplomacy":  4,
    "diplomatic_shifts":  5,
}

# ── schemas ───────────────────────────────────────────────────────────────────
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

GOLD_SCHEMA = pa.schema([
    pa.field("event_date",          pa.date32()),
    pa.field("event_count",         pa.int16()),
    pa.field("max_severity",        pa.int8()),
    pa.field("avg_severity",        pa.float32()),
    pa.field("has_conflict",        pa.bool_()),
    pa.field("top_event_summary",   pa.string()),
    pa.field("top_event_category",  pa.string()),
    pa.field("category_sk",         pa.int8()),
    pa.field("severity_sk",         pa.int8()),
    pa.field("sanctions_count",     pa.int16()),
    pa.field("military_count",      pa.int16()),
    pa.field("security_count",      pa.int16()),
    pa.field("nuclear_count",       pa.int16()),
    pa.field("diplomatic_count",    pa.int16()),
    pa.field("sanctions_max_sev",   pa.int8()),
    pa.field("military_max_sev",    pa.int8()),
    pa.field("security_max_sev",    pa.int8()),
    pa.field("nuclear_max_sev",     pa.int8()),
    pa.field("diplomatic_max_sev",  pa.int8()),
    pa.field("hormuz_flag",         pa.bool_()),
    pa.field("approximate_count",   pa.int16()),
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


def _alert_failures(failed: list, layer: str):
    if not failed:
        return
    message  = f"dwl-news-transform [{layer}] — {len(failed)} item(s) failed:\n\n"
    message += "\n".join(f"  {f}" for f in failed)
    message += "\n\nRerun failed periods manually via Lambda test tab."
    _get_sns_client().publish(
        TopicArn = SNS_TOPIC,
        Subject  = f"[DWL] news-transform {layer}: {len(failed)} failed",
        Message  = message,
    )
    logger.warning("SNS alert sent for %d failed %s items.", len(failed), layer)


# ── bronze loading ────────────────────────────────────────────────────────────

def _load_bronze_day(target_date: date) -> list:
    """Load bronze events for a single day. Returns [] if not found."""
    key = f"{BRONZE_PREFIX}/event_date={target_date.isoformat()}/events.json"
    try:
        obj  = _get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return data.get("events", [])
    except Exception:
        return []


def _load_bronze_month(year: int, month: int) -> list:
    """Load all bronze events for a month."""
    days_in_month = monthrange(year, month)[1]
    all_events    = []
    for day in range(1, days_in_month + 1):
        all_events.extend(_load_bronze_day(date(year, month, day)))
    logger.info("year=%d month=%02d bronze_events=%d", year, month, len(all_events))
    return all_events


# ── normalization ─────────────────────────────────────────────────────────────

def _normalize_event(e: dict) -> dict | None:
    """
    Normalize a bronze event for silver — no filtering, just type coercion.
    Returns None only if event_date is unparseable.
    """
    try:
        event_date = date.fromisoformat(e.get("event_date", ""))
    except ValueError:
        return None

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


# ── silver ────────────────────────────────────────────────────────────────────

def _write_silver(events: list, year: int, month: int) -> str:
    """Write monthly silver Parquet. Returns S3 key."""
    key = f"{SILVER_PREFIX}/year={year}/month={month:02d}/data.parquet"

    if not events:
        table = pa.table(
            {field.name: pa.array([], type=field.type) for field in SILVER_SCHEMA},
            schema=SILVER_SCHEMA,
        )
    else:
        table = pa.Table.from_pylist(events, schema=SILVER_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    _get_s3_client().put_object(
        Bucket      = SILVER_BUCKET,
        Key         = key,
        Body        = buf.read(),
        ContentType = "application/octet-stream",
    )
    logger.info("Silver: wrote %d events to s3://%s/%s", len(events), SILVER_BUCKET, key)
    return key


# ── gold ──────────────────────────────────────────────────────────────────────

def _is_hormuz(event: dict) -> bool:
    location = (event.get("location") or "").lower()
    summary  = (event.get("event_summary") or "").lower()
    return any(kw in location or kw in summary
               for kw in ["hormuz", "strait", "persian gulf", "red sea"])


def _aggregate_day(target_date: date, events: list) -> dict:
    """
    Aggregate a list of gold-filtered events for one day into a single row.
    events should already be filtered (no approximate, no severity 1).
    """
    cat_counts  = defaultdict(int)
    cat_max_sev = defaultdict(int)

    for e in events:
        cat = e.get("event_category", "")
        sev = int(e.get("severity", 0))
        cat_counts[cat]  += 1
        cat_max_sev[cat]  = max(cat_max_sev[cat], sev)

    severities   = [int(e.get("severity", 0)) for e in events]
    hormuz_flag  = any(_is_hormuz(e) for e in events)
    approx_count = sum(1 for e in events if e.get("date_confidence") == "approximate")

    # Top event — highest severity, first by occurrence if tied
    top_event    = max(events, key=lambda e: int(e.get("severity", 0))) if events else None
    top_summary  = top_event.get("event_summary", "") if top_event else ""
    top_category = top_event.get("event_category", "") if top_event else ""

    # Star schema FK lookups
    category_sk = CATEGORY_SK_MAP.get(top_category) if top_category else None
    severity_sk  = max(severities) if severities else None

    return {
        "event_date":         target_date,
        "event_count":        len(events),
        "max_severity":       max(severities) if severities else 0,
        "avg_severity":       round(sum(severities) / len(severities), 2) if severities else 0.0,
        "has_conflict":       len(events) > 0,
        "top_event_summary":  top_summary,
        "top_event_category": top_category,
        "category_sk":        category_sk,
        "severity_sk":        severity_sk,
        "sanctions_count":    cat_counts["sanctions"],
        "military_count":     cat_counts["military_conflict"],
        "security_count":     cat_counts["security_incidents"],
        "nuclear_count":      cat_counts["nuclear_diplomacy"],
        "diplomatic_count":   cat_counts["diplomatic_shifts"],
        "sanctions_max_sev":  cat_max_sev["sanctions"],
        "military_max_sev":   cat_max_sev["military_conflict"],
        "security_max_sev":   cat_max_sev["security_incidents"],
        "nuclear_max_sev":    cat_max_sev["nuclear_diplomacy"],
        "diplomatic_max_sev": cat_max_sev["diplomatic_shifts"],
        "hormuz_flag":        hormuz_flag,
        "approximate_count":  approx_count,
    }


def _write_gold_day(row: dict, target_date: date) -> str:
    """Write daily gold Parquet — one row. Returns S3 key."""
    key   = f"{GOLD_PREFIX}/event_date={target_date.isoformat()}/data.parquet"
    table = pa.Table.from_pylist([row], schema=GOLD_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    _get_s3_client().put_object(
        Bucket      = GOLD_BUCKET,
        Key         = key,
        Body        = buf.read(),
        ContentType = "application/octet-stream",
    )
    logger.info("Gold: wrote %s to s3://%s/%s", target_date, GOLD_BUCKET, key)
    return key


# ── date/month helpers ────────────────────────────────────────────────────────

def _month_ranges(start: date, end: date):
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ── handler ───────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    Routine run  : event = {}  — past 14 days silver + gold
    Backfill run : event = {"backfill": true,
                             "start_date": "2025-01-01",
                             "end_date":   "2026-05-17"}
    """
    today = date.today()

    if event.get("backfill"):
        raw_start = event.get("start_date", "2025-01-01")
        raw_end   = event.get("end_date",   today.isoformat())
        start_dt  = date.fromisoformat(raw_start)
        end_dt    = date.fromisoformat(raw_end)
        logger.info("Backfill mode: %s -> %s", raw_start, raw_end)
    else:
        end_dt   = today - timedelta(days=1)
        start_dt = end_dt - timedelta(days=13)
        logger.info("Routine mode: %s -> %s", start_dt, end_dt)

    silver_results = []
    gold_results   = []
    failed_silver  = []
    failed_gold    = []

    # ── silver (monthly) ──────────────────────────────────────────────────────
    for year, month in _month_ranges(start_dt, end_dt):
        try:
            bronze_events = _load_bronze_month(year, month)
            silver_events = [n for e in bronze_events if (n := _normalize_event(e))]
            dropped       = len(bronze_events) - len(silver_events)

            _write_silver(silver_events, year, month)
            silver_results.append({
                "month":        f"{year}-{month:02d}",
                "status":       "ok",
                "bronze_count": len(bronze_events),
                "silver_count": len(silver_events),
                "dropped":      dropped,
            })
            logger.info("Silver: %d-%02d bronze=%d silver=%d dropped=%d",
                        year, month, len(bronze_events), len(silver_events), dropped)

        except Exception as exc:
            logger.exception("Silver error for %d-%02d", year, month)
            failed_silver.append(f"{year}-{month:02d}")
            silver_results.append({"month": f"{year}-{month:02d}", "status": "error", "error": str(exc)})

    # ── gold (daily) ──────────────────────────────────────────────────────────
    for target_date in _date_range(start_dt, end_dt):
        try:
            bronze_events = _load_bronze_day(target_date)

            # Gold filters: drop approximate + drop severity 1
            gold_events = [
                e for e in bronze_events
                if e.get("date_confidence") != "approximate"
                and int(e.get("severity", 0)) > 1
            ]

            row = _aggregate_day(target_date, gold_events)
            _write_gold_day(row, target_date)
            gold_results.append({
                "date":        target_date.isoformat(),
                "status":      "ok",
                "event_count": len(gold_events),
            })

        except Exception as exc:
            logger.exception("Gold error for %s", target_date)
            failed_gold.append(target_date.isoformat())
            gold_results.append({"date": target_date.isoformat(), "status": "error", "error": str(exc)})

    logger.info(
        "Done. Silver: %d/%d months ok. Gold: %d/%d days ok.",
        sum(1 for r in silver_results if r["status"] == "ok"), len(silver_results),
        sum(1 for r in gold_results   if r["status"] == "ok"), len(gold_results),
    )

    _alert_failures(failed_silver, "silver")
    _alert_failures(failed_gold,   "gold")

    return {
        "statusCode": 200,
        "silver":     silver_results,
        "gold":       gold_results,
    }
