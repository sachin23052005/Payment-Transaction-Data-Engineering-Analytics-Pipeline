import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from validation import validate_transaction

def valid_row():
    return {
        "transaction_id": "T1",
        "customer_id": "C1",
        "merchant_id": "M1",
        "amount": "100",
        "currency": "INR",
        "timestamp": "2026-08-31 10:00:00",
        "location": "Pune",
        "payment_method": "UPI",
        "status": "SUCCESS",
    }

def test_valid_transaction():
    assert validate_transaction(valid_row()) == (True, "valid")

def test_negative_amount_rejected():
    row = valid_row()
    row["amount"] = "-10"
    assert validate_transaction(row)[0] is False

def test_missing_customer_rejected():
    row = valid_row()
    row["customer_id"] = ""
    assert validate_transaction(row)[0] is False

def test_invalid_status_rejected():
    row = valid_row()
    row["status"] = "UNKNOWN"
    assert validate_transaction(row)[0] is False
