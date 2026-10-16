
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, max as spark_max,
    min as spark_min, when, lit, avg
)

spark = (
    SparkSession.builder
    .appName("PurchaseFeatureAnalysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = "file:///home/satwiqreddy/SparkProject/data/events.csv"

events = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(path)
    .select("timestamp", "visitorid", "event", "itemid")
    .dropna(subset=["timestamp", "visitorid", "event", "itemid"])
)

cutoff = 1435708800000
past = events.filter(col("timestamp") < cutoff)
future = events.filter(
    (col("timestamp") >= cutoff) & (col("event") == "transaction")
)

features = past.groupBy("visitorid").agg(
    count("*").alias("total_events"),
    count(when(col("event") == "view", True)).alias("views"),
    count(when(col("event") == "addtocart", True)).alias("carts"),
    countDistinct("itemid").alias("unique_items"),
    countDistinct(when(col("event") == "view", col("itemid"))).alias("unique_viewed"),
    countDistinct(when(col("event") == "addtocart", col("itemid"))).alias("unique_carted"),
    spark_max("timestamp").alias("last_activity"),
    spark_min("timestamp").alias("first_activity")
)

buyers = future.select("visitorid").distinct().withColumn("label", lit(1.0))

data = (
    features.join(buyers, "visitorid", "left")
    .fillna({"label": 0.0})
    .withColumn(
        "days_active",
        (col("last_activity") - col("first_activity")) / lit(86400000.0)
    )
    .withColumn(
        "hours_since_activity",
        (lit(cutoff) - col("last_activity")) / lit(3600000.0)
    )
)

print("\n===== FEATURE COMPARISON BY PURCHASE LABEL =====")
data.groupBy("label").agg(
    count("*").alias("visitors"),
    avg("views").alias("avg_views"),
    avg("carts").alias("avg_carts"),
    avg("total_events").alias("avg_total_events"),
    avg("unique_items").alias("avg_unique_items"),
    avg("unique_viewed").alias("avg_unique_viewed"),
    avg("unique_carted").alias("avg_unique_carted"),
    avg("days_active").alias("avg_days_active"),
    avg("hours_since_activity").alias("avg_hours_since_activity")
).show(truncate=False)

print("\n===== CART ACTIVITY =====")
data.groupBy("label").agg(
    avg(when(col("carts") > 0, 1.0).otherwise(0.0)).alias("fraction_with_cart")
).show()

spark.stop()
