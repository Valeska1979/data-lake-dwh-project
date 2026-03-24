import os
import json
import urllib.request
import psycopg2

def lambda_handler(event, context):

    # API call
    
    API_KEY = os.environ['EIA_API_KEY']

    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?api_key={API_KEY}"
        "&frequency=daily"
        "&data[0]=value"
        "&facets[product][]=EPCBRENT"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        "&offset=0"
        "&length=100"
    )

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read())["response"]["data"]


    # DB connection
  
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        port=5432,
        sslmode="require"
    )

    cur = conn.cursor()

    for row in data:
        cur.execute("""
            INSERT INTO oil_prices (date, price)
            VALUES (%s, %s)
            ON CONFLICT (date) DO NOTHING;
        """, (row["period"], row["value"]))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "statusCode": 200,
        "body": f"Inserted {len(data)} rows"
    }