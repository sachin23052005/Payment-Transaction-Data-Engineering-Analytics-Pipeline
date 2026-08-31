import requests
import csv
from pathlib import Path


BASE = Path(__file__).resolve().parents[1]

API_URL = "http://127.0.0.1:5000/transactions"

OUTPUT_FILE = BASE / "data/raw/api_transactions.csv"


response = requests.get(API_URL, timeout=10)

response.raise_for_status()

data = response.json()

transactions = data["transactions"]

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:

    fieldnames = transactions[0].keys()

    writer = csv.DictWriter(
        f,
        fieldnames=fieldnames
    )

    writer.writeheader()

    writer.writerows(transactions)


print(f"Downloaded {len(transactions)} transactions from API.")

print(f"Saved to: {OUTPUT_FILE}")