"""
dwl-news-load
-------------
Reads gold Parquet files from S3 and upserts into
news_events.fact_conflict_daily in RDS (PostgreSQL).
Includes category_sk and severity_sk for star schema FK support.

Idempotent — uses ON CONFLICT (event_date) DO UPDATE.

Layer                : dwl-stocks-deps
Lambda role          : dwl-news-lambda-role
Runtime              : Python 3.11

Input  : s3://dwl-datapowerchords-gold/news/fact_conflict_daily/
             event_date=<YYYY-MM-DD>/data.parquet

EventBridge schedule : 0 10 ? * MON *  (10:00 UTC every Monday)
Backfill             : invoke with {"backfill": true,
                                    "start_date": "2025-01-01",
                                    "end_date":   "2026-05-17"}
"""

import io
import json
import logging
import os
from datetime import date, timedelta

import boto3
import pyarrow.parquet as pq
import psycopg2
import psycopg2.extras

# ── logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── constants ─────────────────────────────────────────────────────────────────
GOLD_BUCKET   = "dwl-datapowerchords-gold"
GOLD_PREFIX   = "news/fact_conflict_daily"
RDS_SECRET    = "rds!db-9e377778-db31-4627-a0df-da3841e35481"
SNS_TOPIC     = "arn:aws:sns:us-east-1:472069242258:dwl-news-alerts"
REGION        = os.environ.get("AWS_REGION", "us-east-1")
RDS_HOST      = "dwl-shared-pg.copsauwq0r56.us-east-1.rds.amazonaws.com"
RDS_PORT      = 5432
RDS_DB        = "dwl"

# ── clients ───────────────────────────────────────────────────────────────────
_s3_client      = None
_sns_client     = None
_secrets_client = None
_rds_creds      = None


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


def _get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager", region_name=REGION)
    return _secrets_client


def _get_rds_creds() -> dict:
    global _rds_creds
    if _rds_creds is None:
        secret     = _get_secrets_client().get_secret_value(SecretId=RDS_SECRET)
        _rds_creds = json.loads(secret["SecretString"])
        logger.info("RDS credentials loaded from Secrets Manager.")
    return _rds_creds


def _get_connection():
    creds = _get_rds_creds()
    return psycopg2.connect(
        host     = RDS_HOST,
        port     = RDS_PORT,
        dbname   = RDS_DB,
        user     = creds["username"],
        password = creds["password"],
        sslmode  = "require",
    )


def _alert_failures(failed_dates: list):
    if not failed_dates:
        return
    message  = f"dwl-news-load — {len(failed_dates)} date(s) failed:\n\n"
    message += "\n".join(f"  {d}" for d in failed_dates)
    message += "\n\nRerun failed dates manually via Lambda test tab."
    _get_sns_client().publish(
        TopicArn = SNS_TOPIC,
        Subject  = f"[DWL] news-load: {len(failed_dates)} date(s) failed",
        Message  = message,
    )


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_gold_parquet(target_date: date) -> dict | None:
    """Load gold Parquet for a single day. Returns dict of columns or None."""
    key = f"{GOLD_PREFIX}/event_date={target_date.isoformat()}/data.parquet"
    try:
        obj   = _get_s3_client().get_object(Bucket=GOLD_BUCKET, Key=key)
        buf   = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        return table.to_pydict()
    except Exception:
        return None


def _safe_int(val):
    return int(val) if val is not None else None


def _safe_bool(val):
    return bool(val) if val is not None else None


def _upsert_row(conn, row: dict):
    """Idempotent upsert into fact_conflict_daily."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date_sk FROM analytics.dim_date WHERE full_date = %s",
            (row["event_date"][0],),
        )
        result = cur.fetchone()
        if not result:
            logger.warning("No date_sk found for %s — skipping", row["event_date"][0])
            return
        date_sk = result[0]

        cur.execute("""
            INSERT INTO news_events.fact_conflict_daily (
                date_sk, event_date, event_count, max_severity, avg_severity,
                has_conflict, top_event_summary, top_event_category,
                category_sk, severity_sk,
                sanctions_count, military_count, security_count,
                nuclear_count, diplomatic_count,
                sanctions_max_sev, military_max_sev, security_max_sev,
                nuclear_max_sev, diplomatic_max_sev,
                hormuz_flag, approximate_count, ingest_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, NOW()
            )
            ON CONFLICT (event_date) DO UPDATE SET
                date_sk             = EXCLUDED.date_sk,
                event_count         = EXCLUDED.event_count,
                max_severity        = EXCLUDED.max_severity,
                avg_severity        = EXCLUDED.avg_severity,
                has_conflict        = EXCLUDED.has_conflict,
                top_event_summary   = EXCLUDED.top_event_summary,
                top_event_category  = EXCLUDED.top_event_category,
                category_sk         = EXCLUDED.category_sk,
                severity_sk         = EXCLUDED.severity_sk,
                sanctions_count     = EXCLUDED.sanctions_count,
                military_count      = EXCLUDED.military_count,
                security_count      = EXCLUDED.security_count,
                nuclear_count       = EXCLUDED.nuclear_count,
                diplomatic_count    = EXCLUDED.diplomatic_count,
                sanctions_max_sev   = EXCLUDED.sanctions_max_sev,
                military_max_sev    = EXCLUDED.military_max_sev,
                security_max_sev    = EXCLUDED.security_max_sev,
                nuclear_max_sev     = EXCLUDED.nuclear_max_sev,
                diplomatic_max_sev  = EXCLUDED.diplomatic_max_sev,
                hormuz_flag         = EXCLUDED.hormuz_flag,
                approximate_count   = EXCLUDED.approximate_count,
                ingest_timestamp    = NOW()
        """, (
            date_sk,
            row["event_date"][0],
            _safe_int(row["event_count"][0]),
            _safe_int(row["max_severity"][0]),
            float(row["avg_severity"][0]),
            _safe_bool(row["has_conflict"][0]),
            str(row["top_event_summary"][0] or ""),
            str(row["top_event_category"][0] or ""),
            _safe_int(row["category_sk"][0]),
            _safe_int(row["severity_sk"][0]),
            _safe_int(row["sanctions_count"][0]),
            _safe_int(row["military_count"][0]),
            _safe_int(row["security_count"][0]),
            _safe_int(row["nuclear_count"][0]),
            _safe_int(row["diplomatic_count"][0]),
            _safe_int(row["sanctions_max_sev"][0]),
            _safe_int(row["military_max_sev"][0]),
            _safe_int(row["security_max_sev"][0]),
            _safe_int(row["nuclear_max_sev"][0]),
            _safe_int(row["diplomatic_max_sev"][0]),
            _safe_bool(row["hormuz_flag"][0]),
            _safe_int(row["approximate_count"][0]),
        ))
        conn.commit()


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ── handler ───────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    Routine run  : event = {}  — loads past 14 days
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

    results      = []
    failed_dates = []
    conn         = _get_connection()

    try:
        for target_date in _date_range(start_dt, end_dt):
            try:
                row = _load_gold_parquet(target_date)
                if row is None:
                    results.append({"date": target_date.isoformat(), "status": "skipped"})
                    continue

                _upsert_row(conn, row)
                results.append({"date": target_date.isoformat(), "status": "ok"})
                logger.info("Upserted %s", target_date)

            except Exception as exc:
                logger.exception("Error loading %s", target_date)
                failed_dates.append(target_date.isoformat())
                results.append({"date": target_date.isoformat(), "status": "error", "error": str(exc)})

    finally:
        conn.close()

    ok_count = sum(1 for r in results if r["status"] == "ok")
    logger.info("Done. %d/%d dates loaded.", ok_count, len(results))

    _alert_failures(failed_dates)

    return {"statusCode": 200, "results": results}
