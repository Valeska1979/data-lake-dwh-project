import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

API_KEY = os.getenv("WORLD_NEWS_API_KEY")

url = (
    "https://api.worldnewsapi.com/search-news"
    f"?api-key={API_KEY}"
    "&language=en"
    "&sort=publish-time"
    "&sort-direction=DESC"
    "&number=10"
    "&offset=0"
)

response = requests.get(url)

print("Status:", response.status_code)

data = response.json()

# Create DataFrame
df = pd.DataFrame(data["news"])

# Keep relevant columns
df = df[["publish_date", "title", "text", "url", "source_country", "sentiment"]]

print(df.head(10))
print("Rows:", len(df))
