CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    location TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    status TEXT NOT NULL,
    transaction_date TEXT NOT NULL,
    transaction_hour INTEGER NOT NULL,
    is_high_value INTEGER NOT NULL,
    is_failed INTEGER NOT NULL
);
