from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# === CONFIG ===
CSV_DIR = "/data/datasets/globalstay_5tables_bundle"
BRONZE_DIR= "/data/pipeline/bronze"                # output parquet

FILES = {
    "hotels":    "hotels.csv",
    "rooms":     "rooms.csv",
    "customers": "customers.csv",
    "bookings":  "bookings.csv",
    "payments":  "payments.csv",
}

spark = (SparkSession.builder
         .appName("bronze_ingestion")
         .getOrCreate())

spark.conf.set("spark.sql.session.timeZone", "UTC")

for name, fname in FILES.items():
    src = f"{CSV_DIR}/{fname}"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(src)
          .withColumn("ingestion_date", F.current_timestamp()))
    out = f"{BRONZE_DIR}/{name}"
    df.write.mode("overwrite").parquet(out)
    print(f"[BRONZE] {name} -> {out} rows={df.count()}")

spark.stop()
