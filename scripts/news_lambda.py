import json
import urllib3
import os
import psycopg2

http = urllib3.PoolManager()

conn = psycopg2.connect(
    host=os.environ['DB_HOST'],
    database=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    connect_timeout=5
)

def lambda_handler(event, context):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id          BIGINT PRIMARY KEY,
            title       TEXT,
            url         TEXT,
            published_at TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()

    url = f"https://api.worldnewsapi.com/search-news?api-key={os.environ['WORLDNEWS_API_KEY']}&text=technology"
    response = http.request("GET", url)
    data = json.loads(response.data.decode("utf-8"))

    articles = data.get("news", [])

    for article in articles:
        try:
            cursor.execute("""
                INSERT INTO news_articles (id, title, url, published_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                article.get("id"),
                article.get("title"),
                article.get("url"),
                article.get("publish_date")
            ))
        except Exception as e:
            print("Insert error:", e)

    conn.commit()
    cursor.close()

    return {
        "statusCode": 200,
        "body": f"Inserted {len(articles)} articles"
    }
