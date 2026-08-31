import logging
from pathlib import Path

from ingestion import read_transactions, write_csv
from validation import validate_transaction
from transformation import transform_transactions
from loader import load_to_postgres

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data/raw/api_transactions.csv"
PROCESSED = BASE / "data/processed/transactions_processed.csv"
BAD = BASE / "data/bad_records/rejected_transactions.csv"
LOG = BASE / "logs/pipeline.log"

logging.basicConfig(
    filename=LOG,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def deduplicate(rows):
    seen = set()
    unique = []
    duplicates = []

    for row in rows:
        tid = row["transaction_id"]
        if tid in seen:
            duplicates.append(row)
        else:
            seen.add(tid)
            unique.append(row)

    return unique, duplicates

def run_pipeline():
    logging.info("Pipeline started")

    raw = read_transactions(RAW)
    unique, duplicates = deduplicate(raw)

    valid, invalid = [], []
    for row in unique:
        ok, reason = validate_transaction(row)
        if ok:
            valid.append(row)
        else:
            bad = dict(row)
            bad["rejection_reason"] = reason
            invalid.append(bad)

    transformed = transform_transactions(valid)

    fields = [
        "transaction_id","customer_id","merchant_id","amount","currency",
        "timestamp","location","payment_method","status",
        "transaction_date","transaction_hour","is_high_value","is_failed"
    ]

    write_csv(transformed, PROCESSED, fields)

    bad_fields = list(raw[0].keys()) + ["rejection_reason"]
    write_csv(invalid, BAD, bad_fields)

    load_to_postgres(transformed)

    logging.info(
        "Pipeline completed | raw=%d unique=%d duplicates=%d valid=%d invalid=%d",
        len(raw), len(unique), len(duplicates), len(valid), len(invalid)
    )

    print("Pipeline completed successfully.")
    print(f"Raw records       : {len(raw)}")
    print(f"Unique records    : {len(unique)}")
    print(f"Duplicates        : {len(duplicates)}")
    print(f"Valid records     : {len(valid)}")
    print(f"Rejected records  : {len(invalid)}")
    print(f"Processed file    : {PROCESSED}")
    print("PostgreSQL warehouse: localhost:5432/payments")

if __name__ == "__main__":
    run_pipeline()
