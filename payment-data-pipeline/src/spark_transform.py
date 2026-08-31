from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, to_date, when

BASE = Path(__file__).resolve().parents[1]

spark = (
    SparkSession.builder
    .appName("PaymentTransactionETL")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.option("header", True).option("inferSchema", True).csv(
    str(BASE / "data/raw/transactions.csv")
)

clean = (
    df.dropDuplicates(["transaction_id"])
      .filter(col("customer_id").isNotNull())
      .filter(col("amount") > 0)
      .withColumn("timestamp", to_timestamp("timestamp"))
      .withColumn("transaction_date", to_date("timestamp"))
      .withColumn("transaction_hour", hour("timestamp"))
      .withColumn("is_high_value", when(col("amount") >= 10000, True).otherwise(False))
      .withColumn("is_failed", when(col("status") == "FAILED", True).otherwise(False))
)

clean.write.mode("overwrite").parquet(str(BASE / "data/processed/spark_transactions"))

print("PySpark ETL completed.")
print(f"Input rows : {df.count()}")
print(f"Output rows: {clean.count()}")

spark.stop()
