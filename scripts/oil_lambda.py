import os
import json
import urllib.request
import psycopg2
import boto3
import datetime
from collections import defaultdict


def lambda_handler(event, context):

    # -----------------------------
    # API CALL (Brent + WTI from 2026-01-01)
    # -----------------------------
    api_key = os.environ['EIA_API_KEY']

    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?api_key={api_key}"
        "&frequency=daily"
        "&data[0]=value"
        "&facets[product][]=EPCBRENT"
        "&facets[product][]=EPCWTI"
        "&start=2026-01-01"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
    )

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read())["response"]["data"]

    # -----------------------------
    # SAVE RAW TO S3 (PARTITIONED)
    # -----------------------------
    s3 = boto3.client('s3')
    bucket_name = "oil-conflict-prod"

    grouped_data = defaultdict(list)

    # Gruppierung nach Jahr + Monat
    for row in data:
        date_str = row["period"]  # YYYY-MM-DD
        year = date_str[:4]
        month = date_str[5:7]

        key = f"{year}-{month}"
        grouped_data[key].append(row)

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    for key, rows in grouped_data.items():
        year, month = key.split("-")

        s3_key = f"oil/raw-oil/year={year}/month={month}/oil_data_{timestamp}.json"

        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(rows),
            ContentType='application/json'
        )

    # -----------------------------
    # SAVE TO RDS (INCREMENTAL)
    # -----------------------------
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        port=5432,
        sslmode="require"
    )

    cur = conn.cursor()
    inserted_rows = 0

    for row in data:
        cur.execute(
            """
            INSERT INTO oil_prices (date, price, product, unit, source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date, product) DO NOTHING;
            """,
            (
                row["period"],
                row["value"],
                row["product"],
                row.get("units", "USD per barrel"),
                "EIA"
            )
        )
        inserted_rows += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    return {
        "statusCode": 200,
        "body": f"Inserted {inserted_rows} new rows and saved raw data to S3"
    }