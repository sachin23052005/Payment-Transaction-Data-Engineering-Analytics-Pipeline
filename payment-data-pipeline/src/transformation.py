from datetime import datetime

def transform_transactions(rows):
    output = []

    for row in rows:
        item = dict(row)
        item["amount"] = float(item["amount"])

        dt = datetime.strptime(item["timestamp"], "%Y-%m-%d %H:%M:%S")
        item["transaction_date"] = dt.date().isoformat()
        item["transaction_hour"] = dt.hour
        item["is_high_value"] = item["amount"] >= 10000
        item["is_failed"] = item["status"] == "FAILED"

        output.append(item)

    return output
