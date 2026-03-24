import os
import psycopg2
import json
from datetime import datetime

def lambda_handler(event, context):
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, title, url, published_at FROM news_articles LIMIT 10;")
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Convert rows to JSON-serializable format
    result = []
    for row in rows:
        result.append({
            "id": row[0],
            "title": row[1],
            "url": row[2],
            "published_at": row[3].isoformat() if isinstance(row[3], datetime) else row[3]
        })
    
    return {
        "statusCode": 200,
        "body": json.dumps(result)
    }