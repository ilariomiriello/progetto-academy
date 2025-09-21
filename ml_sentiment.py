from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType
from afinn import Afinn

# === CONFIG ===
REVIEWS_CSV = "/data/datasets/globalstay_5tables_bundle/reviews.csv"
GOLD_DIR    = "/data/pipeline/gold"

spark = (SparkSession.builder
         .appName("ml_sentiment_afinn")
         .getOrCreate())

spark.conf.set("spark.sql.session.timeZone", "UTC")

# 1) Leggi reviews CSV
reviews = (spark.read
           .option("header", "true")
           .csv(REVIEWS_CSV))

# 2) UDF con AFINN
af = Afinn(language="en") 
@F.udf(DoubleType())
def sentiment_score(text):
    if text is None:
        return 0.0
    try:
        return float(af.score(text))
    except Exception:
        return 0.0

@F.udf(StringType())
def sentiment_label(score):
    if score > 1.0:
        return "positive"
    elif score < -1.0:
        return "negative"
    else:
        return "neutral"

reviews_scored = (reviews
    .withColumn("sentiment_score", sentiment_score(F.col("review_text")))
    .withColumn("predicted_sentiment", sentiment_label(F.col("sentiment_score"))))

# 3) Scrivi dettaglio recensioni con sentiment (Gold)
(reviews_scored
 .write.mode("overwrite")
 .parquet(f"{GOLD_DIR}/reviews_sentiment"))

# 4) Arricchisci Customer Value (Gold) con avg_sentiment
cust_val = spark.read.parquet(f"{GOLD_DIR}/customer_value")
cust_sent = (reviews_scored.groupBy("customer_id")
             .agg(F.avg("sentiment_score").alias("avg_sentiment_score")))

cust_val_enriched = (cust_val.join(cust_sent, on="customer_id", how="left")
                     .fillna({"avg_sentiment_score": 0.0}))

cust_val_enriched.write.mode("overwrite").parquet(f"{GOLD_DIR}/customer_value_enriched")

spark.stop()
