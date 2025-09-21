from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# === CONFIG ===
SILVER_DIR = "/data/pipeline/silver"
GOLD_DIR   = "/data/pipeline/gold"

COL = {
  "booking_id":  "booking_id",
  "customer_id": "customer_id",
  "room_id":     "room_id",
  "hotel_id":    "hotel_id",
  "checkin":     "checkin_date",
  "checkout":    "checkout_date",
  "total":       "total_amount",
  "amount":      "amount",
  "source":      "source",
  "status":      "status",
}

spark = (SparkSession.builder
         .appName("gold_kpi")
         .getOrCreate())

def rd(name): return spark.read.parquet(f"{SILVER_DIR}/{name}")
def wr(df, name): df.write.mode("overwrite").parquet(f"{GOLD_DIR}/{name}")

bks = rd("bookings")
pmt = rd("payments")

# 1) Daily Revenue
daily_rev = (bks
  .filter( (F.col(COL["status"])=="confirmed") & F.col(COL["total"]).isNotNull() )
  .groupBy(F.col(COL["checkin"]).alias("date"))
  .agg(F.sum(COL["total"]).alias("gross_revenue"),
       F.countDistinct(COL["booking_id"]).alias("bookings_count"))
  .orderBy("date"))
wr(daily_rev, "daily_revenue")

# 2) Cancellation Rate by Source
by_source = (bks.groupBy(COL["source"])
  .agg(F.count("*").alias("total_bookings"),
       F.sum(F.when(F.col(COL["status"])=="cancelled", 1).otherwise(0)).alias("cancelled"))
  .withColumn("cancellation_rate_pct", F.round(F.col("cancelled")/F.col("total_bookings")*100, 2))
  .orderBy(F.col("cancellation_rate_pct").desc()))
wr(by_source, "cancellation_rate_by_source")

# 3) Collection Rate (pagamenti validi)
payments_valid = pmt.filter(
    (~F.col("dq_orphan_payment")) &
    (~F.col("dq_invalid_currency")) &
    (~F.col("dq_over_amount")) &
    F.col(COL["amount"]).isNotNull()
)
bookings_valid = bks.filter(F.col(COL["total"]).isNotNull())

pmt_hotel = (payments_valid
             .join(bookings_valid.select(COL["booking_id"], COL["hotel_id"]), on=COL["booking_id"], how="inner")
             .groupBy(COL["hotel_id"])
             .agg(F.sum(COL["amount"]).alias("total_payments_value")))

bks_hotel = (bookings_valid.groupBy(COL["hotel_id"])
             .agg(F.sum(COL["total"]).alias("total_bookings_value")))

coll = (bks_hotel.join(pmt_hotel, on=COL["hotel_id"], how="left")
        .fillna({"total_payments_value": 0.0})
        .withColumn("collection_rate",
                    F.when(F.col("total_bookings_value")>0,
                           F.round(F.col("total_payments_value")/F.col("total_bookings_value"), 4))
                     .otherwise(F.lit(None))))
wr(coll, "collection_rate")

# 4) Overbooking Alerts
b1 = bks.select(F.col(COL["booking_id"]).alias("booking_id_1"),
                F.col(COL["room_id"]).alias("room_id"),
                F.col(COL["checkin"]).alias("checkin_1"),
                F.col(COL["checkout"]).alias("checkout_1"))
b2 = bks.select(F.col(COL["booking_id"]).alias("booking_id_2"),
                F.col(COL["room_id"]).alias("room_id"),
                F.col(COL["checkin"]).alias("checkin_2"),
                F.col(COL["checkout"]).alias("checkout_2"))
overlap = (b1.join(b2, "room_id")
             .filter(F.col("booking_id_1") < F.col("booking_id_2"))
             .withColumn("overlap_start", F.greatest("checkin_1","checkin_2"))
             .withColumn("overlap_end",   F.least("checkout_1","checkout_2"))
             .filter(F.col("overlap_start") < F.col("overlap_end"))
             .select("room_id","booking_id_1","booking_id_2","overlap_start","overlap_end"))
wr(overlap, "overbooking_alerts")

# 5) Customer Value
cust_val = (bks.filter(F.col(COL["total"]).isNotNull())
            .groupBy(COL["customer_id"])
            .agg(F.countDistinct(COL["booking_id"]).alias("bookings_count"),
                 F.sum(COL["total"]).alias("revenue_sum"),
                 F.round(F.avg(COL["total"]),2).alias("avg_ticket")))
wr(cust_val, "customer_value")

spark.stop()
