import os
import psycopg2

def lambda_handler(event, context):
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )
    cursor = conn.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS news_articles (
        id TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        published_at TIMESTAMP,
        raw JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cursor.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
    finally:
        cursor.close()
        conn.close()
    
    return {"statusCode": 200, "body": "Table created successfully"}