# Payment Transaction Data Engineering & Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.9-orange)](https://spark.apache.org/docs/latest/api/python/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED)](https://www.docker.com/)
[![PyTest](https://img.shields.io/badge/PyTest-4%20Passed-green)](https://pytest.org/)

An end-to-end data engineering pipeline for ingesting, validating, transforming, storing, and analyzing payment transaction data.

The project demonstrates a practical data engineering workflow using **Python, Flask REST APIs, Requests, PySpark, PostgreSQL, SQL, Docker, PyTest, structured logging, and Git**.

The pipeline starts with transaction data exposed through a REST API, consumes the data using Python, performs data-quality validation and duplicate detection, processes the data using PySpark, and stores the final validated dataset in PostgreSQL for analytical queries.

---

## 📌 Project Overview

Modern payment systems generate large volumes of transaction data that must be:

- Ingested reliably
- Validated for data quality
- Checked for duplicate records
- Transformed into a usable format
- Stored persistently
- Tested automatically
- Made available for analytical use cases
- Logged for debugging and operational visibility

This project simulates such a workflow on a smaller scale while keeping the architecture close to a practical data engineering pipeline.

The main objective is to demonstrate how different data engineering components work together as a complete pipeline rather than as isolated scripts.

---

# 🏗️ Architecture

```text
                         PAYMENT TRANSACTION PIPELINE

                              ┌───────────────┐
                              │ Transaction   │
                              │ Raw Dataset   │
                              └───────┬───────┘
                                      │
                                      ▼
                              ┌───────────────┐
                              │   Flask REST  │
                              │      API      │
                              │               │
                              │ /health       │
                              │ /transactions │
                              └───────┬───────┘
                                      │
                                      ▼
                              ┌───────────────┐
                              │ Python API    │
                              │   Ingestion   │
                              │   Requests    │
                              └───────┬───────┘
                                      │
                                      ▼
                         ┌─────────────────────────┐
                         │ api_transactions.csv   │
                         │      Raw API Data      │
                         └────────────┬────────────┘
                                      │
                                      ▼
                         ┌─────────────────────────┐
                         │ Data Validation &       │
                         │ Duplicate Detection     │
                         └────────────┬────────────┘
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                  Valid Records             Rejected Records
                         │                         │
                         ▼                         ▼
                  ┌─────────────┐          ┌──────────────┐
                  │   PySpark   │          │ bad_records/ │
                  │     ETL     │          └──────────────┘
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ PostgreSQL  │
                  │  Database   │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ SQL         │
                  │ Analytics   │
                  └─────────────┘


          ┌────────────┐
          │   PyTest   │ → Automated Testing
          └────────────┘

          ┌────────────┐
          │  Logging   │ → Debugging & Monitoring
          └────────────┘

          ┌────────────┐
          │   Docker   │ → Spark + PostgreSQL Infrastructure
          └────────────┘
```

---

# 🔄 End-to-End Data Flow

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

---

# 🚀 Key Features

## 1. REST API

The project contains a Flask-based REST API that exposes payment transaction data.

### Available endpoints

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

This endpoint can be used to verify that the API service is running.

#### Transaction Endpoint

```http
GET /transactions
```

The endpoint returns the available transaction records in JSON format.

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

---

# 2. API Data Ingestion

The `api_ingestion.py` module consumes the REST API using Python's `requests` library.

The process is:

```text
Flask API
    ↓
HTTP GET request
    ↓
JSON response
    ↓
Extract transactions
    ↓
Write CSV
```

The retrieved data is stored in:

```text
data/raw/api_transactions.csv
```

### Run API ingestion

```powershell
python src/api_ingestion.py
```

Expected output:

```text
Downloaded 13 transactions from API.
Saved to: C:\sachin\PYTHON\payment-data-pipeline\data\raw\api_transactions.csv
```

---

# 3. Data Quality Validation

The pipeline does not assume that every incoming transaction is valid.

The validation layer checks transaction records before they are used for downstream processing.

Examples of data-quality issues handled include:

- Missing transaction information
- Invalid transaction amounts
- Duplicate transactions
- Rejected records

The current dataset contains 13 raw records.

After duplicate detection:

```text
13 raw records
      ↓
12 unique records
      ↓
1 duplicate
```

After validation:

```text
12 unique records
      ↓
10 valid records
      ↓
2 rejected records
```

This provides a basic but practical data-quality workflow.

---

# 4. Duplicate Detection

Duplicate records are identified before the final dataset is stored.

Current execution result:

```text
Raw records       : 13
Unique records    : 12
Duplicates        : 1
Valid records     : 10
Rejected records  : 2
```

The objective is to prevent duplicate transactions from contaminating downstream analytical results.

---

# 5. Rejected Records

Invalid records are not silently ignored.

Rejected records can be stored separately under:

```text
data/bad_records/
```

This allows problematic input data to be inspected independently from the clean dataset.

A production-grade system could later extend this mechanism with:

- Rejection reasons
- Error categories
- Retry mechanisms
- Data-quality reports
- Reprocessing workflows

---

# 6. PySpark ETL

Apache Spark is used for the transformation stage.

The project uses **PySpark** to demonstrate scalable data processing.

The Spark transformation includes:

- Reading transaction data
- Processing transaction records
- Applying transformations
- Counting output records
- Writing processed results

The Spark environment is containerized using Docker.

## Run PySpark

Execute:

```powershell
docker exec -it payment-spark /opt/spark/bin/spark-submit /opt/project/src/spark_transform.py
```

Successful execution produces:

```text
Output rows: 10
```

The Spark application also finishes with:

```text
SparkContext is stopping with exitCode 0
```

`exitCode 0` indicates that the Spark job completed successfully.

---

# 7. PostgreSQL Data Warehouse

PostgreSQL is used as the persistent database for processed payment transactions.

The database runs inside a Docker container.

```text
Docker
   │
   └── PostgreSQL
          │
          └── payments
                 │
                 └── transactions
```

### Database configuration

```text
Database : payments
User     : admin
Port     : 5432
```

## Connect to PostgreSQL

Run:

```powershell
docker exec -it payment-postgres psql -U admin -d payments
```

Then verify the records:

```sql
SELECT COUNT(*) FROM transactions;
```

Expected result:

```text
 count
-------
    10
```

This confirms that the processed transaction records have been loaded into PostgreSQL.

Exit PostgreSQL:

```sql
\q
```

---

# 8. SQL Analytics

Once the transaction data is available in PostgreSQL, SQL can be used to perform analytical operations.

## Total transaction count

```sql
SELECT COUNT(*) AS total_transactions
FROM transactions;
```

## Transactions by status

```sql
SELECT
    status,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY status;
```

This provides information about successful and failed transactions.

## Merchant-level analysis

```sql
SELECT
    merchant_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY merchant_id
ORDER BY total_amount DESC;
```

This can be used to identify merchants with the highest transaction volume or value.

## Location-level analysis

```sql
SELECT
    location,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY location
ORDER BY total_amount DESC;
```

This allows transaction activity to be analyzed by location.

---

# 9. Automated Testing

The project uses **PyTest** for automated testing.

Run:

```powershell
pytest -q
```

Current result:

```text
4 passed
```

Automated testing helps ensure that expected pipeline behavior remains correct when the implementation changes.

Testing is especially useful for:

- Data validation
- Transformation logic
- Utility functions
- Expected pipeline behavior

---

# 10. Logging

The project uses Python's `logging` module for pipeline visibility and debugging.

Example:

```python
logging.info("Reading raw transactions from %s", path)
```

Logs are maintained under:

```text
logs/
```

Logging helps identify:

- Which pipeline stage is executing
- Which input is being processed
- Where failures occur
- Whether expected processing steps were reached

---

# 11. Docker Infrastructure

Docker is used to provide reproducible infrastructure for the project.

Current containers:

```text
┌───────────────────────────────┐
│            Docker             │
│                               │
│  ┌─────────────────────────┐  │
│  │    payment-postgres     │  │
│  │      PostgreSQL 16      │  │
│  └─────────────────────────┘  │
│                               │
│  ┌─────────────────────────┐  │
│  │      payment-spark      │  │
│  │       Spark 3.5.9       │  │
│  └─────────────────────────┘  │
│                               │
└───────────────────────────────┘
```

Docker also avoids the need to configure a complete local Hadoop environment on Windows for the Spark execution.

---

# 📁 Project Structure

```text
payment-data-pipeline/
│
├── data/
│   │
│   ├── raw/
│   │   ├── transactions.csv
│   │   └── api_transactions.csv
│   │
│   ├── processed/
│   │   └── transactions_processed.csv
│   │
│   └── bad_records/
│
├── logs/
│   └── pipeline.log
│
├── sql/
│   └── SQL analytics scripts
│
├── src/
│   ├── api.py
│   ├── api_ingestion.py
│   ├── ingestion.py
│   ├── pipeline.py
│   └── spark_transform.py
│
├── tests/
│   └── test files
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# 🛠️ Technology Stack

| Technology | Role |
|---|---|
| **Python** | Pipeline logic, validation and ingestion |
| **Flask** | REST API |
| **Requests** | API consumption |
| **PySpark** | ETL and data transformation |
| **PostgreSQL** | Persistent transaction storage |
| **SQL** | Analytics and database validation |
| **Docker** | Containerized infrastructure |
| **PyTest** | Automated testing |
| **Logging** | Debugging and execution monitoring |
| **CSV** | Raw/intermediate data storage |
| **Git** | Version control |

---

# 💻 Installation & Setup

## Prerequisites

Install:

- Python 3.10+
- Docker Desktop
- Git

Verify Python:

```powershell
python --version
```

Verify Docker:

```powershell
docker --version
```

Verify Docker Compose:

```powershell
docker compose version
```

---

# ⚙️ Setup

## 1. Clone the repository

```powershell
git clone https://github.com/sachin23052005/Payment-Transaction-Data-Engineering-Analytics-Pipeline.git
```

Navigate into the project:

```powershell
cd Payment-Transaction-Data-Engineering-Analytics-Pipeline
```

---

## 2. Create a Python virtual environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.venv\Scripts\Activate.ps1
```

If using Command Prompt:

```cmd
.venv\Scripts\activate
```

---

## 3. Install dependencies

```powershell
pip install -r requirements.txt
```

---

# 🐳 Start Docker Services

From the project root:

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker ps
```

Expected services:

```text
payment-postgres
payment-spark
```

---

# ▶️ Running the Complete Pipeline

The pipeline should be executed in the following order.

---

## Step 1 — Start the Flask API

Open Terminal 1:

```powershell
python src/api.py
```

Keep this terminal running.

The API should be available at:

```text
http://127.0.0.1:5000
```

---

## Step 2 — Consume the API

Open Terminal 2:

```powershell
python src/api_ingestion.py
```

Expected:

```text
Downloaded 13 transactions from API.
Saved to:
...\data\raw\api_transactions.csv
```

---

## Step 3 — Run the Python pipeline

```powershell
python src/pipeline.py
```

Expected:

```text
Data successfully loaded into PostgreSQL.
Pipeline completed successfully.

Raw records       : 13
Unique records    : 12
Duplicates        : 1
Valid records     : 10
Rejected records  : 2
```

---

## Step 4 — Run PySpark

```powershell
docker exec -it payment-spark /opt/spark/bin/spark-submit /opt/project/src/spark_transform.py
```

Expected:

```text
Output rows: 10
```

A successful Spark shutdown includes:

```text
SparkContext is stopping with exitCode 0
```

---

## Step 5 — Verify PostgreSQL

```powershell
docker exec -it payment-postgres psql -U admin -d payments
```

Run:

```sql
SELECT COUNT(*) FROM transactions;
```

Expected:

```text
 count
-------
    10
```

Exit:

```sql
\q
```

---

## Step 6 — Run automated tests

```powershell
pytest -q
```

Expected:

```text
4 passed
```

---

# 📊 Verified Pipeline Results

The current implementation has been executed successfully with the following results:

| Metric | Result |
|---|---:|
| Raw records | 13 |
| Unique records | 12 |
| Duplicate records | 1 |
| Valid records | 10 |
| Rejected records | 2 |
| PySpark output rows | 10 |
| PostgreSQL records | 10 |
| Automated tests | 4 passed |

The complete data path is:

```text
13 API records
      ↓
12 unique records
      ↓
10 valid records
      ↓
PySpark
      ↓
10 output rows
      ↓
PostgreSQL
      ↓
10 stored records
      ↓
4 automated tests passed
```

---

# 🔍 Data Quality Example

The pipeline demonstrates a basic data-quality workflow:

```text
Incoming Data
     │
     │ 13 records
     ▼
Duplicate Detection
     │
     │ 1 duplicate
     ▼
12 Unique Records
     │
     ▼
Validation
     │
     ├───────────────┐
     ▼               ▼
10 Valid         2 Rejected
     │
     ▼
PySpark Processing
     │
     ▼
PostgreSQL
```

This prevents invalid and duplicate data from directly entering the analytical dataset.

---

# 🧪 Testing Strategy

The project uses automated tests to validate expected behavior.

Run all tests with:

```powershell
pytest -q
```

Current status:

```text
4 passed
```

The testing layer provides a safety net when modifying ingestion, validation, or transformation logic.

---

# 🐞 Debugging

When troubleshooting the project, useful commands include:

### Check Docker containers

```powershell
docker ps
```

### Check PostgreSQL logs

```powershell
docker logs payment-postgres
```

### Check Spark container

```powershell
docker logs payment-spark
```

### Run tests

```powershell
pytest -q
```

### Check PySpark version

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

---

# 🧠 Engineering Concepts Demonstrated

This project demonstrates practical understanding of several core data engineering concepts.

## Data ingestion

Data is obtained from a REST API rather than being consumed only from a static database.

## ETL

The pipeline follows:

```text
Extract
  ↓
Transform
  ↓
Load
```

## Data quality

Records are validated before being used for downstream analytics.

## Deduplication

Duplicate transactions are identified and removed from the valid dataset.

## Distributed processing

PySpark is used for transformation and processing.

## Relational storage

Processed data is stored in PostgreSQL.

## Analytical querying

SQL is used to generate transaction, merchant, and location-level insights.

## Containerization

Docker provides reproducible Spark and PostgreSQL environments.

## Automated testing

PyTest validates expected application behavior.

## Logging

Logging provides visibility into pipeline execution and assists debugging.

---

# 📈 Initial Version vs Current Implementation

## Initial approach

```text
CSV
 ↓
Python
 ↓
Basic database processing
```

## Current implementation

```text
REST API
   ↓
Python Requests
   ↓
Raw Transaction Data
   ↓
Validation
   ↓
Deduplication
   ↓
PySpark
   ↓
PostgreSQL
   ↓
SQL Analytics
```

Additional engineering capabilities:

```text
Docker
PyTest
Logging
Git/GitHub
```

---

# 🔐 Configuration and Security Notes

This project is intended as a portfolio and learning implementation.

For a production system, database credentials and configuration values should not be hard-coded.

A production implementation could use:

- Environment variables
- `.env` files excluded through `.gitignore`
- Secret managers
- Cloud IAM
- Database connection pooling
- Role-based database access
- Separate development and production configurations

The current implementation uses local configuration for simplicity.

---

# 🚧 Future Improvements

The current pipeline is functional, but several improvements could be added later.

## Cloud storage

Move raw and processed datasets to object storage such as Amazon S3.

## Streaming

Replace batch API/CSV ingestion with a streaming transaction source.

## Pipeline orchestration

Introduce a workflow orchestration system if the pipeline grows into multiple scheduled production jobs.

## CI/CD

Automatically run tests whenever code is pushed to GitHub.

## Data-quality reporting

Generate detailed reports showing rejected records and rejection reasons.

## Dashboard

Add a lightweight analytics dashboard showing:

- Total transactions
- Successful transactions
- Failed transactions
- Transaction value
- Merchant activity
- Location activity

These are future extensions and are not required for the current implementation.

---

# 🎯 Project Objectives

The project was developed to demonstrate the ability to:

1. Build a complete data ingestion pipeline.
2. Consume data through a REST API.
3. Perform data-quality validation.
4. Detect and remove duplicate records.
5. Process data using PySpark.
6. Store processed data in PostgreSQL.
7. Perform SQL-based analytics.
8. Containerize infrastructure using Docker.
9. Implement automated testing using PyTest.
10. Use logging for debugging and monitoring.
11. Manage the project using Git and GitHub.

---

# 📌 Project Status

## Implemented

```text
REST API                    ✅
API data ingestion          ✅
Python ETL                  ✅
Data validation             ✅
Duplicate detection         ✅
Rejected records            ✅
PySpark transformation      ✅
PostgreSQL storage          ✅
SQL analytics               ✅
Docker                      ✅
Automated testing           ✅
Logging                     ✅
Git/GitHub                  ✅
```

## Verified

```text
13 raw records
12 unique records
1 duplicate
10 valid records
2 rejected records
10 PySpark output rows
10 PostgreSQL records
4 automated tests passed
```

---

# 👨‍💻 Author

**Sachin Prasad**

### Payment Transaction Data Engineering & Analytics Pipeline

Core technologies:

```text
Python
PySpark
PostgreSQL
SQL
Flask
Docker
PyTest
Git
```

---

# ⭐ Summary

This project demonstrates an end-to-end payment transaction data engineering workflow:

```text
                  ┌─────────────┐
                  │ REST API    │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ Python      │
                  │ Ingestion   │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ Data        │
                  │ Quality     │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ PySpark     │
                  │ ETL         │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ PostgreSQL  │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │ SQL         │
                  │ Analytics   │
                  └─────────────┘

             Docker | PyTest | Logging | Git
```

The pipeline has been executed end-to-end and verified with successful API ingestion, data-quality processing, PySpark execution, PostgreSQL loading, and automated tests.

---

## 🔗 Repository

GitHub:
https://github.com/sachin23052005/Payment-Transaction-Data-Engineering-Analytics-Pipeline
