
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit

spark = (
    SparkSession.builder
    .appName("CheckPurchaseWindows")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA = "file:///home/satwiqreddy/SparkProject/data/events.csv"

events = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA)
    .select("timestamp", "visitorid", "event")
    .dropna(subset=["timestamp", "visitorid", "event"])
    .cache()
)

print("Minimum timestamp:", events.agg({"timestamp": "min"}).first()[0])
print("Maximum timestamp:", events.agg({"timestamp": "max"}).first()[0])

# Candidate prediction dates, in epoch milliseconds.
# Each date must have a full 7-day future observation period.
dates = [
    ("2015-05-01", 1430438400000),
    ("2015-06-01", 1433116800000),
    ("2015-07-01", 1435708800000),
]

WINDOW_MS = 7 * 24 * 60 * 60 * 1000

for name, cutoff in dates:
    past_visitors = (
        events.filter(col("timestamp") < cutoff)
        .select("visitorid")
        .distinct()
    )

    future_buyers = (
        events.filter(
            (col("timestamp") >= cutoff)
            & (col("timestamp") < cutoff + WINDOW_MS)
            & (col("event") == "transaction")
        )
        .select("visitorid")
        .distinct()
    )

    eligible = past_visitors.join(future_buyers, "visitorid", "left")

    eligible_count = past_visitors.count()
    buyer_count = past_visitors.join(
        future_buyers, "visitorid", "inner"
    ).count()

    print(
        f"{name} | eligible visitors={eligible_count:,} "
        f"| buyers with prior activity={buyer_count:,}"
    )

events.unpersist()
spark.stop()
