# Payment Transaction Data Engineering & Analytics Pipeline
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.9-orange)](https://spark.apache.org/docs/latest/api/python/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED)](https://www.docker.com/)
[![PyTest](https://img.shields.io/badge/PyTest-4%20Passed-green)](https://pytest.org/)

An end-to-end data engineering project for ingesting, validating, transforming, storing, and analyzing payment transaction data.

The project demonstrates two complementary data-processing workflows:

1. A **Python + Flask REST API pipeline** for API-based ingestion, validation, duplicate detection, transformation, and PostgreSQL loading.
2. A **large-scale PySpark pipeline** for processing a 1-million-row transaction dataset, performing ETL, storing processed data in Parquet, loading it into PostgreSQL using Spark JDBC, and performing SQL-based analytics.

The project uses **Python, Flask, PySpark, Apache Spark, PostgreSQL, SQL, Docker, Parquet, JDBC, PyTest, and Git**.

---

## Project Overview

Payment transaction systems generate large volumes of transactional data that need to be cleaned, validated, transformed, stored, and analyzed reliably.

This project implements a complete data engineering workflow for payment transaction data.

The primary large-scale pipeline processes approximately **1 million transaction records** using Apache Spark and performs:

- CSV ingestion
- Data cleaning
- Data validation
- Duplicate detection
- Transaction transformation
- Feature generation
- Parquet storage
- PostgreSQL loading using Spark JDBC
- SQL-based analytics

The project also contains a smaller Flask-based REST API pipeline that demonstrates API ingestion and traditional Python-based data processing.

---

## Architecture

The project contains two complementary pipelines.

```text
                         PAYMENT TRANSACTION DATA
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
          REST API Pipeline                 Large-Scale Pipeline
                 │                                 │
             Flask API                         1M-row CSV
                 │                                 │
          API Ingestion                         PySpark
                 │                                 │
          Raw CSV Data                  Cleaning & Validation
                 │                                 │
           Validation                    Deduplication
                 │                                 │
          Transformation                 Transformation
                 │                                 │
             PySpark                          Parquet
                 │                                 │
                 │                           Spark JDBC
                 │                                 │
                 └───────────────┬─────────────────┘
                                 │
                            PostgreSQL
                                 │
                           SQL Analytics
```

---

## End-to-End Data Flow

### Original API-Based Pipeline

```text
REST API
   ↓
API Ingestion
   ↓
Raw Transaction Data
   ↓
Validation
   ↓
Duplicate Detection
   ↓
Transformation
   ↓
PySpark Processing
   ↓
PostgreSQL
   ↓
SQL Analytics
```

### Large-Scale Spark Pipeline

```text
1,000,000-row CSV
        ↓
     PySpark
        ↓
Data Cleaning & Validation
        ↓
   Deduplication
        ↓
   Transformation
        ↓
      Parquet
        ↓
   Spark JDBC
        ↓
   PostgreSQL
        ↓
   SQL Analytics
```

---

## Key Features

### 1. Large-Scale Spark Processing

The primary pipeline processes a 1-million-row transaction dataset using Apache Spark and PySpark.

The Spark ETL process performs:

- CSV ingestion using PySpark
- Data cleaning
- Data validation
- Duplicate detection
- Timestamp conversion
- Transaction date extraction
- Transaction hour extraction
- High-value transaction identification
- Failed transaction identification
- Parquet output
- PostgreSQL loading using Spark JDBC

### 2. REST API

The project contains a Flask-based REST API that exposes payment transaction data.

#### Health Check

```http
GET /health
```

Example response:

```json
{
    "status": "healthy",
    "service": "payment-data-pipeline"
}
```

#### Transaction Endpoint

```http
GET /transactions
```

Example structure:

```json
{
    "count": 13,
    "transactions": [
        {
            "amount": "4500",
            "currency": "INR",
            "customer_id": "C101",
            "location": "Mumbai",
            "merchant_id": "M501",
            "payment_method": "CARD",
            "status": "SUCCESS"
        }
    ]
}
```

### 3. API Data Ingestion

The `api_ingestion.py` module consumes the Flask REST API using Python's `requests` library.

```text
Flask API
    ↓
HTTP GET Request
    ↓
JSON Response
    ↓
Extract Transactions
    ↓
Write CSV
```

The resulting API data is stored as:

```text
data/raw/api_transactions.csv
```

### 4. Data Validation

The pipeline performs data-quality validation before loading transaction records.

Validation checks include:

- Required fields
- Transaction amount
- Currency
- Transaction status
- Payment method
- Missing values
- Invalid transaction records

Supported currencies:

```text
INR
USD
EUR
```

Supported statuses:

```text
SUCCESS
FAILED
```

Supported payment methods:

```text
CARD
UPI
NETBANKING
```

### 5. Duplicate Detection

Duplicate transaction records are identified using the transaction ID.

The pipeline ensures that transaction IDs remain unique in the processed dataset.

The large-scale Spark pipeline also performs deduplication before the processed data is written to Parquet.

### 6. Data Transformation

The pipeline transforms raw transaction data into an analytics-ready format.

Transformations include:

- Converting transaction amounts to numeric values
- Parsing transaction timestamps
- Creating transaction dates
- Extracting transaction hours
- Identifying high-value transactions
- Identifying failed transactions

The large-scale Spark pipeline produces:

```text
transaction_date
transaction_hour
is_high_value
is_failed
```

A transaction is considered high-value when:

```text
amount >= 10000
```

### 7. Large-Scale PySpark ETL

```text
transactions_1m.csv
        ↓
Spark CSV Reader
        ↓
Data Cleaning
        ↓
Validation
        ↓
Deduplication
        ↓
Transformation
        ↓
Parquet
```

Implementation:

```text
src/spark_large_transform.py
```

Output:

```text
data/processed/transactions_1m/
```

### 8. PostgreSQL Data Storage

PostgreSQL is used as the relational database for storing processed transaction data.

The project uses two tables:

```text
transactions
```

Used by the original Python/API pipeline.

```text
transactions_1m
```

Used by the large-scale Spark pipeline.

### 9. Spark JDBC Loading

The large-scale processed Parquet dataset is loaded into PostgreSQL using Spark JDBC.

Implementation:

```text
src/load_to_postgres.py
```

Run:

```bash
docker exec payment-spark sh -c "mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4 /opt/project/src/load_to_postgres.py"
```

### 10. SQL Analytics

#### Total Transactions

```sql
SELECT COUNT(*) AS total_transactions
FROM transactions_1m;
```

#### Total Transaction Amount

```sql
SELECT SUM(amount) AS total_amount
FROM transactions_1m;
```

#### Average Transaction Amount

```sql
SELECT AVG(amount) AS average_amount
FROM transactions_1m;
```

#### Transactions by Status

```sql
SELECT
    status,
    COUNT(*) AS transaction_count
FROM transactions_1m
GROUP BY status;
```

#### Transactions by Payment Method

```sql
SELECT
    payment_method,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions_1m
GROUP BY payment_method
ORDER BY total_amount DESC;
```

#### Transactions by Location

```sql
SELECT
    location,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions_1m
GROUP BY location
ORDER BY total_amount DESC;
```

#### Failed Transactions

```sql
SELECT COUNT(*) AS failed_transactions
FROM transactions_1m
WHERE is_failed = TRUE;
```

#### High-Value Transactions

```sql
SELECT COUNT(*) AS high_value_transactions
FROM transactions_1m
WHERE is_high_value = TRUE;
```

---

## Large-Scale Pipeline Results

The large-scale pipeline successfully processed the generated transaction dataset and loaded the processed records into PostgreSQL.

| Metric | Result |
|---|---:|
| PostgreSQL rows | 994,997 |
| Unique transaction IDs | 994,997 |
| Invalid amounts | 0 |
| Null transaction IDs | 0 |
| Null customer IDs | 0 |
| Null timestamps | 0 |
| SUCCESS transactions | 945,183 |
| FAILED transactions | 49,814 |
| Minimum amount | 50.06 |
| Maximum amount | 49,999.96 |
| Average amount | 25,013.05 |
| High-value transactions | 796,496 |
| Failed transactions | 49,814 |
| Total transaction amount | 24,887,907,584.38 |

### Payment Method Distribution

| Payment Method | Transactions |
|---|---:|
| CARD | 249,659 |
| UPI | 248,618 |
| WALLET | 248,207 |
| NET_BANKING | 248,513 |

---

## Testing

The project uses **PyTest** for automated testing.

Tests are located in:

```text
tests/
```

Run:

```bash
pytest
```

Expected result:

```text
4 passed
```

---

## Docker

Docker provides the project infrastructure.

```text
Docker Compose
      │
      ├── PostgreSQL
      │
      └── Spark
```

Start:

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

Stop:

```bash
docker compose down
```

---

## Project Structure

```text
payment-data-pipeline/
│
├── data/
│   ├── raw/
│   │   ├── transactions.csv
│   │   └── transactions_1m.csv
│   │
│   └── processed/
│       └── transactions_1m/
│
├── src/
│   ├── api.py
│   ├── api_ingestion.py
│   ├── ingestion.py
│   ├── validation.py
│   ├── transformation.py
│   ├── loader.py
│   ├── pipeline.py
│   ├── spark_transform.py
│   ├── spark_large_transform.py
│   └── load_to_postgres.py
│
├── tests/
│   └── test_validation.py
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

### Important Files

| File | Purpose |
|---|---|
| `api.py` | Flask REST API |
| `api_ingestion.py` | API-based transaction ingestion |
| `ingestion.py` | CSV ingestion utilities |
| `validation.py` | Transaction validation |
| `transformation.py` | Transaction transformation |
| `pipeline.py` | Original Python pipeline |
| `spark_transform.py` | Spark processing for the original workflow |
| `spark_large_transform.py` | Large-scale 1M-row Spark ETL |
| `load_to_postgres.py` | Spark JDBC PostgreSQL loader |
| `loader.py` | PostgreSQL loader for the original pipeline |
| `test_validation.py` | PyTest validation tests |
| `docker-compose.yml` | Spark and PostgreSQL infrastructure |

---

## Dataset

The project contains a smaller transaction dataset for the API/Python workflow and a larger synthetic transaction dataset for Spark processing.

The large dataset contains approximately:

```text
1,000,000 transaction records
```

Schema:

```text
transaction_id
customer_id
merchant_id
amount
currency
timestamp
location
payment_method
status
```

The large generated dataset is excluded from Git because of its size.

---

## Running the Project

### Option 1 — Original API/Python Pipeline

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Start the Flask API:

```bash
python src/api.py
```

API URL:

```text
http://127.0.0.1:5000
```

Ingest API data:

```bash
python src/api_ingestion.py
```

Run the Python pipeline:

```bash
python src/pipeline.py
```

Run tests:

```bash
pytest
```

### Option 2 — Large-Scale Spark Pipeline

Start Docker:

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

Run Spark ETL:

```bash
docker exec -it payment-spark /opt/spark/bin/spark-submit /opt/project/src/spark_large_transform.py
```

Output:

```text
data/processed/transactions_1m/
```

Load into PostgreSQL:

```bash
docker exec payment-spark sh -c "mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4 /opt/project/src/load_to_postgres.py"
```

Connect to PostgreSQL:

```bash
docker exec -it payment-postgres psql -U admin -d payments
```

Verify:

```sql
SELECT COUNT(*)
FROM transactions_1m;
```

Expected:

```text
994997
```

---

## Data Processing Pipeline Summary

```text
                    Raw CSV
                       │
                       ▼
                 Apache Spark
                       │
                       ▼
               Data Cleaning
                       │
                       ▼
                 Validation
                       │
                       ▼
               Deduplication
                       │
                       ▼
               Transformation
                       │
                       ▼
                   Parquet
                       │
                       ▼
                 Spark JDBC
                       │
                       ▼
                  PostgreSQL
                       │
                       ▼
                 SQL Analytics
```

---

## Data Quality Workflow

```text
Raw Transactions
       │
       ▼
Required Field Validation
       │
       ▼
Amount Validation
       │
       ▼
Currency Validation
       │
       ▼
Status Validation
       │
       ▼
Payment Method Validation
       │
       ▼
Duplicate Detection
       │
       ▼
Clean Transaction Dataset
```

---

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Pipeline logic and data processing |
| Flask | REST API |
| Requests | API data ingestion |
| PySpark | Distributed ETL processing |
| Apache Spark | Large-scale data processing |
| Parquet | Processed data storage |
| PostgreSQL | Relational database |
| Spark JDBC | Spark-to-PostgreSQL data loading |
| SQL | Data analytics |
| Docker | Infrastructure and environment |
| PyTest | Automated testing |
| Git | Version control |

---

## Engineering Practices

The project demonstrates:

- ETL pipeline design
- Data validation
- Data quality checks
- Duplicate detection
- Distributed data processing
- Large-scale CSV processing
- Parquet-based data storage
- JDBC database integration
- SQL analytics
- API-based ingestion
- Automated testing
- Docker-based infrastructure
- Structured pipeline components
- Version control using Git

---

## Future Improvements

Potential future improvements include:

- Cloud deployment
- Incremental data processing
- Pipeline orchestration
- Automated CI/CD
- Advanced data-quality monitoring
- Database indexing optimization
- Additional analytical dashboards
- Production-grade configuration and secret management
- Performance benchmarking with larger datasets

---

## Conclusion

The **Payment Transaction Data Engineering & Analytics Pipeline** demonstrates an end-to-end data engineering workflow for payment transaction data.

The project combines a Python/Flask API-based pipeline with a scalable Apache Spark pipeline capable of processing approximately 1 million transaction records.

The primary scalable workflow demonstrates:

```text
CSV
 ↓
PySpark
 ↓
Data Cleaning & Validation
 ↓
Transformation
 ↓
Parquet
 ↓
Spark JDBC
 ↓
PostgreSQL
 ↓
SQL Analytics
```

The project provides practical experience with data ingestion, ETL, data quality, distributed processing, analytical storage, database integration, testing, Docker, and SQL-based analytics.
