import logging

REQUIRED_FIELDS = [
    "transaction_id", "customer_id", "merchant_id", "amount",
    "currency", "timestamp", "location", "payment_method", "status"
]

VALID_STATUSES = {"SUCCESS", "FAILED"}
VALID_CURRENCIES = {"INR", "USD", "EUR"}
VALID_PAYMENT_METHODS = {"CARD", "UPI", "NETBANKING"}

def validate_transaction(row):
    """Return (is_valid, reason)."""
    for field in REQUIRED_FIELDS:
        if row.get(field) in (None, ""):
            return False, f"missing_{field}"

    try:
        amount = float(row["amount"])
    except (TypeError, ValueError):
        return False, "invalid_amount"

    if amount <= 0:
        return False, "non_positive_amount"

    if row["currency"] not in VALID_CURRENCIES:
        return False, "invalid_currency"

    if row["status"] not in VALID_STATUSES:
        return False, "invalid_status"

    if row["payment_method"] not in VALID_PAYMENT_METHODS:
        return False, "invalid_payment_method"

    return True, "valid"
