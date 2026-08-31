import os
import psycopg2


def load_to_postgres(rows):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "payments"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123")
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            merchant_id TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            location TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            status TEXT NOT NULL,
            transaction_date DATE NOT NULL,
            transaction_hour INTEGER NOT NULL,
            is_high_value BOOLEAN NOT NULL,
            is_failed BOOLEAN NOT NULL
        )
    """)

    for r in rows:
        cur.execute("""
            INSERT INTO transactions (
                transaction_id,
                customer_id,
                merchant_id,
                amount,
                currency,
                timestamp,
                location,
                payment_method,
                status,
                transaction_date,
                transaction_hour,
                is_high_value,
                is_failed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id)
            DO UPDATE SET
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                is_failed = EXCLUDED.is_failed
        """, (
            r["transaction_id"],
            r["customer_id"],
            r["merchant_id"],
            r["amount"],
            r["currency"],
            r["timestamp"],
            r["location"],
            r["payment_method"],
            r["status"],
            r["transaction_date"],
            r["transaction_hour"],
            r["is_high_value"],
            r["is_failed"]
        ))

    conn.commit()

    cur.close()
    conn.close()

    print("Data successfully loaded into PostgreSQL.")