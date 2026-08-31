-- 1. Total successful transaction value
SELECT ROUND(SUM(amount), 2) AS successful_value
FROM transactions
WHERE status = 'SUCCESS';

-- 2. Transaction count by status
SELECT status, COUNT(*) AS transaction_count
FROM transactions
GROUP BY status;

-- 3. Top merchants by successful transaction value
SELECT merchant_id,
       ROUND(SUM(amount), 2) AS total_value
FROM transactions
WHERE status = 'SUCCESS'
GROUP BY merchant_id
ORDER BY total_value DESC
LIMIT 10;

-- 4. Failed transaction rate
SELECT ROUND(
    100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*),
    2
) AS failed_rate_percent
FROM transactions;

-- 5. High-value transactions
SELECT transaction_id, customer_id, merchant_id, amount, location
FROM transactions
WHERE is_high_value = 1
ORDER BY amount DESC;
