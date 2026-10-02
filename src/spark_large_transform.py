from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    trim,
    to_timestamp,
    to_date,
    when,
    hour
)


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("PaymentTransactionLargeETL")
    .getOrCreate()
)


# ============================================================
# 2. INPUT / OUTPUT PATHS
# ============================================================

INPUT = "/opt/project/data/raw/transactions_1m.csv"
OUTPUT = "/opt/project/data/processed/transactions_1m"


# ============================================================
# 3. START MESSAGE
# ============================================================

print("\n========================================")
print(" PAYMENT TRANSACTION LARGE ETL")
print("========================================")

print(f"Input file  : {INPUT}")
print(f"Output path : {OUTPUT}")


# ============================================================
# 4. READ 1M TRANSACTION DATASET
# ============================================================

print("\nReading 1M transaction dataset...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(INPUT)
)

print(f"Input columns: {len(df.columns)}")
print(f"Columns: {df.columns}")


# ============================================================
# 5. CLEAN TRANSACTION AMOUNTS
# ============================================================

print("\nCleaning transaction amounts...")

clean = df.withColumn(
    "amount_clean",
    regexp_replace(
        trim(col("amount")),
        r"[$,]",
        ""
    ).cast("double")
)


# ============================================================
# 6. PARSE TRANSACTION TIMESTAMP
# ============================================================

print("Parsing transaction timestamps...")

clean = clean.withColumn(
    "transaction_timestamp",
    to_timestamp(
        trim(col("timestamp"))
    )
)


# ============================================================
# 7. VALIDATE TRANSACTION RECORDS
# ============================================================

print("Validating transaction records...")

clean = clean.withColumn(
    "valid_record",
    when(
        col("transaction_id").isNotNull()
        & (trim(col("transaction_id")) != "")
        & col("customer_id").isNotNull()
        & (trim(col("customer_id")) != "")
        & col("merchant_id").isNotNull()
        & (trim(col("merchant_id")) != "")
        & col("amount_clean").isNotNull()
        & (col("amount_clean") > 0)
        & col("transaction_timestamp").isNotNull()
        & col("currency").isNotNull()
        & (trim(col("currency")) != "")
        & col("status").isNotNull()
        & (trim(col("status")) != ""),
        True
    )
    .otherwise(False)
)


# ============================================================
# 8. CREATE TRANSACTION STATUS FLAGS
# ============================================================

clean = (
    clean
    .withColumn(
        "is_success",
        when(
            trim(col("status")) == "SUCCESS",
            True
        )
        .otherwise(False)
    )
    .withColumn(
        "is_failed",
        when(
            trim(col("status")) == "FAILED",
            True
        )
        .otherwise(False)
    )
    .withColumn(
        "transaction_hour",
        hour(col("transaction_timestamp"))
    )
)


# ============================================================
# 9. COUNT INPUT RECORDS
# ============================================================

print("\nCounting input records...")

total_rows = clean.count()

print(f"Raw records: {total_rows:,}")


# ============================================================
# 10. REMOVE DUPLICATE TRANSACTIONS
# ============================================================

print("\nChecking for duplicate transaction IDs...")

deduplicated = clean.dropDuplicates(
    ["transaction_id"]
)

unique_rows = deduplicated.count()

duplicate_rows = total_rows - unique_rows


# ============================================================
# 11. DATA QUALITY METRICS
# ============================================================

print("\nCalculating data-quality metrics...")


# Valid records
valid_rows = (
    deduplicated
    .filter(col("valid_record") == True)
    .count()
)


# Invalid records
invalid_rows = unique_rows - valid_rows


# Successful transactions
successful_rows = (
    deduplicated
    .filter(col("is_success") == True)
    .count()
)


# Failed transactions
failed_rows = (
    deduplicated
    .filter(col("is_failed") == True)
    .count()
)


# ============================================================
# 12. DISPLAY QUALITY REPORT
# ============================================================

print("\n========================================")
print(" PAYMENT TRANSACTION QUALITY REPORT")
print("========================================")

print(f"Raw records       : {total_rows:,}")
print(f"Unique records    : {unique_rows:,}")
print(f"Duplicates        : {duplicate_rows:,}")
print(f"Valid records     : {valid_rows:,}")
print(f"Invalid records   : {invalid_rows:,}")
print(f"Successful        : {successful_rows:,}")
print(f"Failed            : {failed_rows:,}")

print("========================================")


# ============================================================
# 13. CREATE PRODUCTION-READY DATASET
# ============================================================

print("\nCreating production-ready dataset...")

final_df = (
    deduplicated
    .filter(col("valid_record") == True)
    .select(
        # Original transaction fields
        col("transaction_id"),
        col("customer_id"),
        col("merchant_id"),

        # Cleaned amount
        col("amount_clean").alias("amount"),

        # Other original fields
        col("currency"),

        # Parsed timestamp
        col("transaction_timestamp").alias("timestamp"),

        col("location"),
        col("payment_method"),
        col("status"),

        # Derived fields
        to_date(
            col("transaction_timestamp")
        ).alias("transaction_date"),

        hour(
            col("transaction_timestamp")
        ).alias("transaction_hour"),

        when(
            col("amount_clean") >= 10000,
            True
        )
        .otherwise(False)
        .alias("is_high_value"),

        col("is_failed")
    )
)


# ============================================================
# 14. DISPLAY FINAL SCHEMA
# ============================================================

print("\nFinal dataset schema:")

final_df.printSchema()


# ============================================================
# 15. WRITE PARQUET OUTPUT
# ============================================================

print(f"\nWriting Parquet output to: {OUTPUT}")

(
    final_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT)
)


# ============================================================
# 16. COMPLETION MESSAGE
# ============================================================

print("\nParquet write completed successfully.")

print("\n========================================")
print(" ETL COMPLETED SUCCESSFULLY")
print("========================================")


# ============================================================
# 17. STOP SPARK
# ============================================================

spark.stop()