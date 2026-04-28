import os
import json
import urllib.request
import boto3
import datetime


def lambda_handler(event, context):

    # -----------------------------
    # API CALL
    # -----------------------------
    api_key = os.environ['EIA_API_KEY']

    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?api_key={api_key}"
        "&frequency=daily"
        "&data[0]=value"
        "&facets[product][]=EPCBRENT"
        "&facets[product][]=EPCWTI"
        "&start=2025-01-01"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
    )

    print("Calling EIA API...")   # LOG 1

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read())["response"]["data"]

    print(f"Fetched {len(data)} rows from EIA API")   # LOG 2

    # -----------------------------
    # SAVE RAW TO S3 (BRONZE)
    # -----------------------------
    s3 = boto3.client('s3')
    bucket_name = "dwl-datapowerchords-raw"

    ingest_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    s3_key = f"oil/eia/ingest_date={ingest_date}/eia_data.json"

    print(f"Saving data to S3 path: {s3_key}")   # LOG 3

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType='application/json'
    )

    print("Upload to S3 successful")   # LOG 4

    return {
        "statusCode": 200,
        "body": f"Saved {len(data)} raw rows to Bronze layer"
    }