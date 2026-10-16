
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("RetailrocketEDA")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = "file:///home/satwiqreddy/SparkProject/data/events.csv"

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(path)
)

print("\n===== DATASET OVERVIEW =====")
print("Total rows:", df.count())
print("Total columns:", len(df.columns))
print("Columns:", df.columns)

print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== MISSING VALUES =====")
missing = df.select([
    spark_sum(
        col(c).isNull().cast("long")
    ).alias(c)
    for c in df.columns
])
missing.show(truncate=False)

print("\n===== UNIQUE COUNTS =====")
df.select(
    countDistinct("visitorid").alias("unique_visitors"),
    countDistinct("itemid").alias("unique_items"),
    countDistinct("transactionid").alias("unique_transactions")
).show()

print("\n===== EVENT DISTRIBUTION =====")
event_counts = df.groupBy("event").count().orderBy(col("count").desc())
event_counts.show()

print("\n===== EVENT PERCENTAGES =====")
total = df.count()
event_counts.withColumn(
    "percentage",
    col("count") * 100.0 / total
).show(truncate=False)

print("\n===== SAMPLE RECORDS =====")
df.show(10, truncate=False)

spark.stop()
