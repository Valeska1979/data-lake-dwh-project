"""
dwl-conflict-ingest
-------------------
Uses Gemini 2.0 Flash with Google Search grounding to research and classify
Iran-region geopolitical conflict events, writing structured results to S3.

No external packages required — uses only Python stdlib (urllib) + boto3
(built into the Lambda runtime). No additional Lambda layer needed.

Fetch strategy  : weekly chunks — better recall than daily, exact dates per event
Severity scale  : 1 (routine) → 5 (extreme)
Date confidence : exact | approximate — approximate excluded downstream in W3

EventBridge schedule : 0 6 ? * MON *  (06:00 UTC every Monday)
Backfill             : invoke with {"backfill": true,
                                    "start_date": "2025-01-01",
                                    "end_date":   "2025-04-30"}
                       Split into monthly invocations to avoid timeout.
Lambda role          : dwl-news-lambda-role
Secret               : dwl/news/gemini  ->  {"api_key": "..."}

Output S3 path
    s3://dwl-datapowerchords-raw/conflict/gemini/
        ingest_week=<YYYY-MM-DD>/events.json
"""

import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import date, timedelta

import boto3

# ── logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── constants ─────────────────────────────────────────────────────────────────
SECRET_NAME  = "dwl/news/gemini"
BUCKET       = "dwl-datapowerchords-raw"
S3_PREFIX    = "news/gemini"
GEMINI_MODEL = "gemini-2.0-flash"
MAX_EVENTS   = 15
REGION       = os.environ.get("AWS_REGION", "eu-central-1")

VALID_CATEGORIES = {
    "sanctions",
    "military_conflict",
    "security_incidents",
    "nuclear_diplomacy",
    "diplomatic_shifts",
}

PROMPT_TEMPLATE = """You are a geopolitical intelligence analyst specialising in Iran and \
Middle East oil-market risk. Research and return the most significant Iran-region \
geopolitical conflict events that occurred between {start_date} and {end_date} inclusive.

SCOPE
- Primary actor: Iran (including IRGC, Iranian proxies)
- Region: Iran, Iraq, Yemen, Saudi Arabia, UAE, Oman, Israel, Lebanon, Syria, \
Kuwait, Bahrain, Qatar, Strait of Hormuz, Red Sea, Persian Gulf
- Relevant actors: Iran, IRGC, Houthi, Hezbollah, Israel, United States, Saudi Arabia, UAE

CATEGORIES
  sanctions          - Economic/financial pressure tools: sanctions, tariffs, export restrictions.
  military_conflict  - Direct use of armed force: strikes, battles, raids, cross-border clashes.
  security_incidents - Non-traditional security events: cyberattacks, maritime disruptions, \
tanker seizures, terrorism, sabotage.
  nuclear_diplomacy  - Nuclear programme diplomacy, IAEA inspections, negotiations, escalation signals.
  diplomatic_shifts  - Summits, ceasefires, alliance shifts, normalisation, breakdown of relations.

SEVERITY SCALE (1-5)
  1 - Routine    : diplomatic statement, minor patrol incident, low-level skirmish
  2 - Notable    : new sanctions package, small-scale strike, naval warning
  3 - Significant: confirmed airstrike, tanker seizure, IAEA escalation signal
  4 - Severe     : major strike campaign, large-scale attack with casualties, Hormuz threat
  5 - Extreme    : direct Iran-US or Iran-Israel exchange, Hormuz closure, nuclear red line crossed

DATE CONFIDENCE
  exact       - confirmed date from a dated news source
  approximate - event is real but exact date uncertain

RULES
- Return only the top {max_events} most significant events for the period.
- Only include events with a clear Iran/proxy connection or direct oil-market relevance.
- Each event maps to exactly one category.
- event_summary must be 8 words or fewer as a compact dashboard tag.
- Only include date_confidence = "exact" unless severity >= 4.
- CRITICAL: If no relevant events occurred, you MUST return an empty JSON array [] and nothing else. Never return prose or explanation.
- Return ONLY a valid JSON array. No markdown fences. No explanation. No backticks.

OUTPUT FORMAT
[
  {{
    "event_date": "YYYY-MM-DD",
    "event_category": "<category>",
    "event_summary": "<compact tag>",
    "severity": <1|2|3|4|5>,
    "date_confidence": "<exact|approximate>",
    "actors": ["actor1", "actor2"],
    "location": "<country or region>",
    "description": "<1-2 sentence factual description>",
    "source_hint": "<publication or outlet>"
  }}
]"""

# ── clients ───────────────────────────────────────────────────────────────────
_secrets_client = None
_s3_client      = None
_api_key        = None


def _get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager", region_name=REGION)
    return _secrets_client


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=REGION)
    return _s3_client


def _get_api_key() -> str:
    global _api_key
    if _api_key is None:
        secret  = _get_secrets_client().get_secret_value(SecretId=SECRET_NAME)
        _api_key = json.loads(secret["SecretString"])["GEMINI_API_KEY"]
        logger.info("Gemini API key loaded from Secrets Manager.")
    return _api_key


# ── helpers ───────────────────────────────────────────────────────────────────

def _event_hash(event: dict) -> str:
    key = f"{event.get('event_date', '')}-{event.get('event_summary', '')}"
    return hashlib.md5(key.encode()).hexdigest()


def _week_ranges(start: date, end: date):
    current = start
    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        yield current, week_end
        current = week_end + timedelta(days=1)


def _fetch_events_for_week(api_key: str, week_start: date, week_end: date) -> list:
    """Call Gemini 2.0 Flash via REST with Google Search grounding."""
    start_str = week_start.isoformat()
    end_str   = week_end.isoformat()

    prompt  = PROMPT_TEMPLATE.format(
        start_date=start_str,
        end_date=end_str,
        max_events=MAX_EVENTS,
    )

    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "tools":    [{"google_search": {}}],
        "generationConfig": {
            "temperature":     0.0,
            "maxOutputTokens": 2048,
        },
    }).encode("utf-8")

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={api_key}"
    )

    req = urllib.request.Request(
        url,
        data    = payload,
        headers = {"Content-Type": "application/json"},
        method  = "POST",
    )

    logger.info("Calling Gemini for week %s -> %s", start_str, end_str)

    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    raw = result["candidates"][0]["content"]["parts"][0]["text"].strip()
    logger.info("Gemini raw response (first 300 chars): %s", raw[:300])

    # Strip markdown fences defensively
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    events = json.loads(raw)
    logger.info("week=%s->%s raw_events=%d", start_str, end_str, len(events))
    return events


def _validate_events(events: list, week_start: str, week_end: str) -> list:
    validated = []
    for e in events:
        if e.get("event_category") not in VALID_CATEGORIES:
            logger.warning("Unknown category '%s' — skipping", e.get("event_category"))
            continue
        if e.get("severity") not in (1, 2, 3, 4, 5):
            logger.warning("Invalid severity '%s' — skipping", e.get("severity"))
            continue
        e["url_hash"]      = _event_hash(e)
        e["ingest_period"] = f"{week_start}/{week_end}"
        validated.append(e)
    return validated


def _write_to_s3_by_date(events: list, week_start: date, week_end: date) -> list:
    """
    Group events by event_date and write one file per date —
    consistent with oil/stock ingest_date=YYYY-MM-DD partitioning.
    Writes an empty file for dates in the week with no events.
    Returns list of S3 keys written.
    """
    # Group events by event_date
    from collections import defaultdict
    by_date = defaultdict(list)
    for e in events:
        by_date[e.get("event_date", week_start.isoformat())].append(e)

    # Ensure every date in the week has a file (even if empty)
    current = week_start
    while current <= week_end:
        date_str = current.isoformat()
        if date_str not in by_date:
            by_date[date_str] = []
        current += timedelta(days=1)

    keys_written = []
    for date_str, day_events in sorted(by_date.items()):
        key  = f"{S3_PREFIX}/ingest_date={date_str}/events.json"
        body = json.dumps(
            {
                "ingest_date": date_str,
                "event_count": len(day_events),
                "events":      day_events,
            },
            ensure_ascii=False,
            indent=2,
        )
        _get_s3_client().put_object(
            Bucket      = BUCKET,
            Key         = key,
            Body        = body.encode("utf-8"),
            ContentType = "application/json",
        )
        logger.info("Wrote %d events to s3://%s/%s", len(day_events), BUCKET, key)
        keys_written.append(key)

    return keys_written


# ── handler ───────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    Routine run  : event = {}
                   Fetches the 7 days ending yesterday.
                   Runs weekly via EventBridge — no overlap, no gaps.

    Backfill run : event = {"backfill": true,
                             "start_date": "2025-01-01",
                             "end_date":   "2025-01-31"}
                   Split into monthly invocations to avoid Lambda timeout.
    """
    api_key = _get_api_key()
    today   = date.today()

    if event.get("backfill"):
        raw_start = event.get("start_date", "2025-01-01")
        raw_end   = event.get("end_date",   today.isoformat())
        start_dt  = date.fromisoformat(raw_start)
        end_dt    = date.fromisoformat(raw_end)
        logger.info("Backfill mode: %s -> %s", raw_start, raw_end)
    else:
        end_dt   = today - timedelta(days=1)
        start_dt = end_dt - timedelta(days=6)
        logger.info("Routine mode: %s -> %s", start_dt, end_dt)

    results = []

    for week_start, week_end in _week_ranges(start_dt, end_dt):
        try:
            events    = _fetch_events_for_week(api_key, week_start, week_end)
            validated = _validate_events(events, week_start.isoformat(), week_end.isoformat())
            keys      = _write_to_s3_by_date(validated, week_start, week_end)

            results.append({
                "week":        week_start.isoformat(),
                "status":      "ok",
                "event_count": len(validated),
                "files_written": len(keys),
            })

            time.sleep(5)   # avoid Gemini rate limiting

        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                logger.warning("Rate limit hit for week %s — waiting 30s and retrying once", week_start)
                time.sleep(30)
                try:
                    events    = _fetch_events_for_week(api_key, week_start, week_end)
                    validated = _validate_events(events, week_start.isoformat(), week_end.isoformat())
                    keys      = _write_to_s3_by_date(validated, week_start, week_end)
                    results.append({
                        "week":          week_start.isoformat(),
                        "status":        "ok",
                        "event_count":   len(validated),
                        "files_written": len(keys),
                    })
                except Exception as retry_exc:
                    logger.error("Retry failed for week %s — writing empty files: %s", week_start, retry_exc)
                    keys = _write_to_s3_by_date([], week_start, week_end)
                    results.append({"week": week_start.isoformat(), "status": "empty_response", "event_count": 0, "files_written": len(keys)})
            else:
                logger.error("Gemini HTTP error for week %s: %s", week_start, exc)
                results.append({"week": week_start.isoformat(), "status": "http_error", "error": str(exc)})
        except json.JSONDecodeError as exc:
            logger.error("JSON parse error for week %s — Gemini returned non-JSON, storing empty week: %s", week_start, exc)
            validated = []
            keys      = _write_to_s3_by_date(validated, week_start, week_end)
            results.append({
                "week":          week_start.isoformat(),
                "status":        "empty_response",
                "event_count":   0,
                "files_written": len(keys),
            })
        except Exception as exc:
            logger.exception("Unexpected error for week %s", week_start)
            results.append({"week": week_start.isoformat(), "status": "error", "error": str(exc)})

    ok_count = sum(1 for r in results if r["status"] == "ok")
    logger.info("Done. %d/%d weeks succeeded.", ok_count, len(results))

    return {"statusCode": 200, "results": results}
