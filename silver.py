from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# === CONFIG ===
BRONZE_DIR = "/data/pipeline/bronze"
SILVER_DIR = "/data/pipeline/silver"

COL = {
  "booking_id":  "booking_id",
  "customer_id": "customer_id",
  "room_id":     "room_id",
  "hotel_id":    "hotel_id",
  "checkin":     "checkin_date",
  "checkout":    "checkout_date",
  "total":       "total_amount",
  "currency":    "currency",
  "status":      "status",   
  "source":      "source",
  "email":       "email",
  "country":     "Country",  
  "amount":      "amount",
}

VALID_CURRENCY = ["EUR", "USD", "GBP"]

spark = (SparkSession.builder
         .appName("silver_cleaning")
         .getOrCreate())

spark.conf.set("spark.sql.session.timeZone", "UTC")

def read_bronze(name):
    return spark.read.parquet(f"{BRONZE_DIR}/{name}")

def write_silver(df, name):
    df.write.mode("overwrite").parquet(f"{SILVER_DIR}/{name}")

# 1) HOTELS: rimuovi Country='XX'
df_hotels = read_bronze("hotels")
if COL["country"] in df_hotels.columns:
    df_hotels = df_hotels.filter(F.col(COL["country"]) != "XX")
write_silver(df_hotels, "hotels")

# 2) CUSTOMERS: email vuote -> NULL, dedup per customer_id
df_customers = read_bronze("customers")
if COL["email"] in df_customers.columns:
    df_customers = df_customers.withColumn(
        COL["email"],
        F.when(F.trim(F.col(COL["email"])).isin("", "null", "NULL"), F.lit(None)).otherwise(F.col(COL["email"]))
    )
if COL["customer_id"] in df_customers.columns:
    if "ingestion_date" in df_customers.columns:
        w = Window.partitionBy(COL["customer_id"]).orderBy(F.col("ingestion_date").desc())
        df_customers = (df_customers
                        .withColumn("_rn", F.row_number().over(w))
                        .filter(F.col("_rn")==1)
                        .drop("_rn"))
    else:
        df_customers = df_customers.dropDuplicates([COL["customer_id"]])
write_silver(df_customers, "customers")

# 3) ROOMS: dedup per room_id
df_rooms = read_bronze("rooms")
if COL["room_id"] in df_rooms.columns:
    df_rooms = df_rooms.dropDuplicates([COL["room_id"]])
write_silver(df_rooms, "rooms")

# 4) BOOKINGS: fix date invertite, total negativi -> NULL, currency valida
df_bookings = read_bronze("bookings")

# converti date se sono stringhe
for c in (COL["checkin"], COL["checkout"]):
    if c in df_bookings.columns and dict(df_bookings.dtypes).get(c) == "string":
        df_bookings = df_bookings.withColumn(c, F.to_date(F.col(c)))

# flag e swap date
if COL["checkin"] in df_bookings.columns and COL["checkout"] in df_bookings.columns:
    df_bookings = (df_bookings
                   .withColumn("dq_swapped_dates", F.when(F.col(COL["checkin"]) > F.col(COL["checkout"]), True).otherwise(False))
                   .withColumn("_in",  F.least(F.col(COL["checkin"]), F.col(COL["checkout"])))
                   .withColumn("_out", F.greatest(F.col(COL["checkin"]), F.col(COL["checkout"])))
                   .drop(COL["checkin"], COL["checkout"])
                   .withColumnRenamed("_in", COL["checkin"])
                   .withColumnRenamed("_out", COL["checkout"]))

# total negativi -> NULL + flag
if COL["total"] in df_bookings.columns:
    df_bookings = (df_bookings
                   .withColumn("dq_negative_total", F.when(F.col(COL["total"]) < 0, True).otherwise(False))
                   .withColumn(COL["total"], F.when(F.col(COL["total"]) < 0, F.lit(None)).otherwise(F.col(COL["total"]))))

# currency valida -> altrimenti NULL + flag
if COL["currency"] in df_bookings.columns:
    df_bookings = (df_bookings
                   .withColumn("dq_invalid_currency",
                               F.when(~F.col(COL["currency"]).isin(VALID_CURRENCY), True).otherwise(False))
                   .withColumn(COL["currency"],
                               F.when(~F.col(COL["currency"]).isin(VALID_CURRENCY), F.lit(None)).otherwise(F.col(COL["currency"]))))

write_silver(df_bookings, "bookings")

# 5) PAYMENTS: orphan / over_amount / currency invalida
df_payments = read_bronze("payments")

bk = spark.read.parquet(f"{SILVER_DIR}/bookings").select(
    F.col(COL["booking_id"]).alias("bk_booking_id"),
    F.col(COL["total"]).alias("bk_total_amount")
) if COL["booking_id"] in df_bookings.columns and COL["total"] in df_bookings.columns else None

p = df_payments.alias("p")
if bk is not None and COL["booking_id"] in df_payments.columns:
    p = p.join(bk, F.col(f"p.{COL['booking_id']}")==F.col("bk_booking_id"), "left")
    p = p.withColumn("dq_orphan_payment", F.when(F.col("bk_booking_id").isNull(), True).otherwise(False))
    if COL["amount"] in p.columns:
        p = p.withColumn("dq_over_amount",
                         F.when((F.col("bk_booking_id").isNotNull()) & (F.col(COL["amount"]) > F.col("bk_total_amount")), True)
                          .otherwise(False))
    p = p.drop("bk_booking_id")

# currency non valida
if COL["currency"] in p.columns:
    p = (p.withColumn("dq_invalid_currency",
                      F.when(~F.col(COL["currency"]).isin(VALID_CURRENCY), True).otherwise(False))
         .withColumn(COL["currency"],
                      F.when(~F.col(COL["currency"]).isin(VALID_CURRENCY), F.lit(None)).otherwise(F.col(COL["currency"]))))

write_silver(p, "payments")

spark.stop()
