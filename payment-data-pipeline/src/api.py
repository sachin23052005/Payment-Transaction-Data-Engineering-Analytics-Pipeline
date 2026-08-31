from flask import Flask, jsonify
import csv
from pathlib import Path

app = Flask(__name__)

BASE = Path(__file__).resolve().parents[1]
DATA_FILE = BASE / "data/raw/transactions.csv"


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "payment-data-pipeline"
    })


@app.get("/transactions")
def transactions():

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        data = list(csv.DictReader(f))

    return jsonify({
        "count": len(data),
        "transactions": data
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)