import io
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


RAW_BUCKET = os.environ["RAW_BUCKET"]
CURATED_BUCKET = os.environ["CURATED_BUCKET"]
GOLD_BUCKET = os.environ["GOLD_BUCKET"]

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

SERIES = ["EPCBRENT", "EPCWTI"]

PRODUCT_MAP = {
    "EPCBRENT": "Brent",
    "EPCWTI": "WTI",
}

SOURCE = "EIA"

s3 = boto3.client("s3")
sns = boto3.client("sns")


SILVER_SCHEMA = pa.schema([
    ("trade_date", pa.date32()),
    ("price", pa.decimal128(18, 6)),
    ("ingest_timestamp", pa.timestamp("us", tz="UTC")),
    ("source", pa.string()),
])

GOLD_SCHEMA = pa.schema([
    ("price_sk", pa.int64()),
    ("date_sk", pa.int32()),
    ("price", pa.decimal128(18, 6)),
    ("oil_type", pa.string()),
    ("ingest_timestamp", pa.timestamp("us", tz="UTC")),
    ("source", pa.string()),
])


def publish_failure(subject, message):

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )


def to_decimal_18_6(value):

    return Decimal(str(value)).quantize(
        Decimal("0.000001"),
        rounding=ROUND_HALF_UP
    )


def latest_ingest_date(series_id):

    prefix = f"oil/eia/series_id={series_id}/"

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    dates = set()

    for page in paginator.paginate(
        Bucket=RAW_BUCKET,
        Prefix=prefix
    ):

        for obj in page.get("Contents", []):

            parts = obj["Key"].split("/")

            for part in parts:

                if part.startswith("ingest_date="):

                    dates.add(
                        part.split("=", 1)[1]
                    )

    if not dates:

        raise RuntimeError(
            f"No bronze partitions for {series_id}"
        )

    return max(dates)


def read_bronze(series_id, ingest_date):

    key = (
        f"oil/eia/"
        f"series_id={series_id}/"
        f"ingest_date={ingest_date}/"
        f"eia.json"
    )

    print(f"Reading bronze: {key}")

    obj = s3.get_object(
        Bucket=RAW_BUCKET,
        Key=key
    )

    payload = json.loads(
        obj["Body"].read()
    )

    rows = payload["rows"]

    if not rows:

        raise RuntimeError(
            f"Empty bronze rows for {series_id}"
        )

    return rows


def parquet_bytes(rows, schema):

    table = pa.Table.from_pylist(
        rows,
        schema=schema
    )

    buffer = io.BytesIO()

    pq.write_table(
        table,
        buffer,
        compression="snappy"
    )

    return buffer.getvalue()


def lambda_handler(event, context):

    try:

        ingest_timestamp = datetime.now(
            timezone.utc
        )

        silver_partitioned = defaultdict(list)
        gold_partitioned = defaultdict(list)

        all_rows = []

        for series_id in SERIES:

            ingest_date = latest_ingest_date(
                series_id
            )

            rows = read_bronze(
                series_id,
                ingest_date
            )

            all_rows.extend(rows)

        print(f"Loaded {len(all_rows)} bronze rows")

        for row in all_rows:

            if (
                not row.get("value")
                or not row.get("period")
                or not row.get("product")
            ):
                continue

            oil_type = PRODUCT_MAP.get(
                row["product"]
            )

            if not oil_type:
                continue

            try:

                price_decimal = to_decimal_18_6(
                    row["value"]
                )

            except Exception:
                continue

            try:

                date_obj = datetime.strptime(
                    row["period"],
                    "%Y-%m-%d"
                )

            except Exception:
                continue

            trade_date = date_obj.date()

            year = date_obj.year

            month = f"{date_obj.month:02d}"

            date_sk = int(
                date_obj.strftime("%Y%m%d")
            )

            price_sk = int(
                f"{date_sk}{1 if oil_type == 'Brent' else 2}"
            )

            silver_partitioned[
                (oil_type, year, month)
            ].append({
                "trade_date": trade_date,
                "price": price_decimal,
                "ingest_timestamp": ingest_timestamp,
                "source": SOURCE,
            })

            gold_partitioned[
                trade_date
            ].append({
                "price_sk": price_sk,
                "date_sk": date_sk,
                "price": price_decimal,
                "oil_type": oil_type,
                "ingest_timestamp": ingest_timestamp,
                "source": SOURCE,
            })

        print(
            f"Created {len(silver_partitioned)} silver partitions"
        )

        print(
            f"Created {len(gold_partitioned)} gold partitions"
        )

        # =====================================================
        # WRITE SILVER
        # =====================================================

        silver_written = 0

        for (
            oil_type,
            year,
            month
        ), rows in silver_partitioned.items():

            body = parquet_bytes(
                rows,
                SILVER_SCHEMA
            )

            key = (
                f"oil/prices/"
                f"oil_type={oil_type}/"
                f"year={year}/"
                f"month={month}/"
                f"prices.parquet"
            )

            s3.put_object(
                Bucket=CURATED_BUCKET,
                Key=key,
                Body=body,
                ContentType="application/octet-stream"
            )

            silver_written += 1

            print(
                f"Silver → s3://{CURATED_BUCKET}/{key}"
            )

        # =====================================================
        # WRITE GOLD
        # =====================================================

        gold_written = 0

        for trade_date, rows in gold_partitioned.items():

            body = parquet_bytes(
                rows,
                GOLD_SCHEMA
            )

            key = (
                f"oil/fact_oil_prices/"
                f"trade_date={trade_date.isoformat()}/"
                f"part-0000.parquet"
            )

            s3.put_object(
                Bucket=GOLD_BUCKET,
                Key=key,
                Body=body,
                ContentType="application/octet-stream"
            )

            gold_written += 1

        print(
            f"Gold → {gold_written} partitions written"
        )

        return {
            "silver_files_written": silver_written,
            "gold_partitions_written": gold_written,
            "rows_processed": len(all_rows)
        }

    except Exception as e:

        publish_failure(
            subject="Oil Transform Failed",
            message=str(e)
        )

        raise