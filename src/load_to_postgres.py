from pyspark.sql import SparkSession


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("PaymentTransactionPostgresLoader")
    .getOrCreate()
)


# ============================================================
# Configuration
# ============================================================

INPUT = "/opt/project/data/processed/transactions_1m"

JDBC_URL = "jdbc:postgresql://payment-postgres:5432/payments"

PROPERTIES = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

TABLE_NAME = "transactions_1m"


# ============================================================
# Read processed Parquet
# ============================================================

print("\nReading processed Parquet data...")

df = spark.read.parquet(INPUT)

print(f"Columns: {df.columns}")

print("\nWriting data to PostgreSQL...")


# ============================================================
# Write to PostgreSQL
# ============================================================

(
    df.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", TABLE_NAME)
    .option("user", PROPERTIES["user"])
    .option("password", PROPERTIES["password"])
    .option("driver", PROPERTIES["driver"])
    .option("batchsize", 10000)
    .option("numPartitions", 4)
    .mode("overwrite")
    .save()
)


print("\n========================================")
print(" POSTGRESQL LOAD COMPLETED")
print("========================================")


spark.stop()