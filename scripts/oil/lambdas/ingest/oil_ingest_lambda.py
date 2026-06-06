
import json
import logging
import os
import urllib.request
from datetime import date, timedelta

import boto3

# ------------------------------------------------------------------
# config
# ------------------------------------------------------------------

BUCKET = os.environ["BUCKET"]
EIA_API_KEY = os.environ["EIA_API_KEY"]

BACKFILL_START = os.environ.get(
    "BACKFILL_START",
    "2025-01-01"
)

ROUTINE_LOOKBACK_DAYS = int(
    os.environ.get(
        "ROUTINE_LOOKBACK_DAYS",
        "7"
    )
)

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

REGION = os.environ.get(
    "AWS_REGION",
    "us-east-1"
)

SOURCE = "EIA"

SERIES = [
    "EPCBRENT",
    "EPCWTI"
]

# ------------------------------------------------------------------
# clients
# ------------------------------------------------------------------

s3 = boto3.client("s3")
sns = boto3.client(
    "sns",
    region_name=REGION
)

logger = logging.getLogger()
logger.setLevel(
    os.environ.get(
        "LOG_LEVEL",
        "INFO"
    )
)

# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def publish_failure(subject, message):

    try:

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )

    except Exception:
        logger.exception(
            "Failed sending SNS alert"
        )


def fetch_data(
    start_date,
    end_date=None
):

    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?api_key={EIA_API_KEY}"
        "&frequency=daily"
        "&data[0]=value"
        "&facets[product][]=EPCBRENT"
        "&facets[product][]=EPCWTI"
        f"&start={start_date}"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
    )

    if end_date:
        url += f"&end={end_date}"

    logger.info(
        "Calling EIA API"
    )

    with urllib.request.urlopen(
        url,
        timeout=60
    ) as response:

        payload = json.loads(
            response.read()
        )

    rows = payload["response"]["data"]

    logger.info(
        "Fetched %s rows",
        len(rows)
    )

    return rows


def split_rows(rows):

    result = {
        "EPCBRENT": [],
        "EPCWTI": []
    }

    for row in rows:

        product = row.get("product")

        if product in result:
            result[product].append(row)

    return result


def write_series_file(
    series_id,
    ingest_date,
    start_date,
    rows
):

    body = {
        "meta": {
            "series_id": series_id,
            "ingest_date": ingest_date,
            "start_date": start_date,
            "source": SOURCE,
            "row_count": len(rows)
        },
        "rows": rows
    }

    key = (
        f"oil/eia/"
        f"series_id={series_id}/"
        f"ingest_date={ingest_date}/"
        f"eia.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(
        "Wrote %s rows to %s",
        len(rows),
        key
    )

    return key


# ------------------------------------------------------------------
# handler
# ------------------------------------------------------------------

def lambda_handler(event, context):

    try:

        ingest_date = date.today().isoformat()

        # ----------------------------------------------------------
        # BACKFILL
        # ----------------------------------------------------------

        if event.get("backfill"):

            start_date = event.get(
                "start_date",
                BACKFILL_START
            )

            end_date = event.get(
                "end_date",
                ingest_date
            )

            logger.info(
                "Backfill mode %s -> %s",
                start_date,
                end_date
            )

        # ----------------------------------------------------------
        # ROUTINE
        # ----------------------------------------------------------

        else:

            end_dt = date.today()

            start_dt = (
                end_dt
                - timedelta(
                    days=ROUTINE_LOOKBACK_DAYS
                )
            )

            start_date = start_dt.isoformat()
            end_date = end_dt.isoformat()

            logger.info(
                "Routine mode %s -> %s",
                start_date,
                end_date
            )

        rows = fetch_data(
            start_date=start_date,
            end_date=end_date
        )

        split = split_rows(rows)

        results = []

        for series_id in SERIES:

            key = write_series_file(
                series_id=series_id,
                ingest_date=ingest_date,
                start_date=start_date,
                rows=split[series_id]
            )

            results.append({
                "series_id": series_id,
                "rows": len(split[series_id]),
                "key": key
            })

        return {
            "statusCode": 200,
            "results": results
        }

    except Exception as e:

        logger.exception(
            "Oil ingest failed"
        )

        publish_failure(
            subject="Oil Ingest Failed",
            message=str(e)
        )

        raise
