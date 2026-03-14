import requests
import pandas as pd
import os
from dotenv import load_dotenv

# .env laden
load_dotenv()

API_KEY = os.getenv("EIA_API_KEY")

url = (
    "https://api.eia.gov/v2/petroleum/pri/spt/data/"
    f"?api_key={API_KEY}"
    "&frequency=daily"
    "&data[0]=value"
    "&facets[product][]=EPCBRENT"
    "&sort[0][column]=period"
    "&sort[0][direction]=desc"
    "&offset=0"
    "&length=5000"
)

response = requests.get(url)

print("Status:", response.status_code)

data = response.json()

# DataFrame erstellen
df = pd.DataFrame(data["response"]["data"])

# Nur relevante Spalten
df = df[["period", "value"]]

print(df.head())
print("Rows:", len(df))