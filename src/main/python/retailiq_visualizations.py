
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, hour, from_unixtime
)
import matplotlib.pyplot as plt
import os

# --------------------------------------------------
# INITIALIZATION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RetailIQ_Visualizations")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

DATA_PATH = (
    "file:///home/satwiqreddy/SparkProject/data/events.csv"
)

OUTPUT_DIR = (
    "/home/satwiqreddy/SparkProject/"
    "review_evidence/visualizations"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
    .filter(col("visitorid").isNotNull())
)

print("Dataset loaded:", df.count(), "events")


def save_chart(filename):
    plt.tight_layout()
    plt.savefig(
        os.path.join(OUTPUT_DIR, filename),
        dpi=150,
        bbox_inches="tight"
    )
    plt.show()
    plt.close()
    print("Saved:", filename)


# --------------------------------------------------
# CHART 1: EVENT TYPE DISTRIBUTION
# --------------------------------------------------

print("\nChart 1: Event distribution")

event_counts = (
    df.groupBy("event")
    .count()
    .orderBy("count", ascending=False)
    .toPandas()
)

plt.figure(figsize=(8, 5))
bars = plt.bar(event_counts["event"], event_counts["count"])

plt.title("RetailIQ: Distribution of E-commerce Events")
plt.xlabel("Event Type")
plt.ylabel("Number of Events")
plt.grid(axis="y", alpha=0.3)

for bar in bars:
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height(),
        f"{int(bar.get_height()):,}",
        ha="center",
        va="bottom"
    )

save_chart("01_event_distribution.png")


# --------------------------------------------------
# CHART 2: VISITOR ENGAGEMENT DISTRIBUTION
# --------------------------------------------------

print("\nChart 2: Visitor engagement")

visitor_events = df.groupBy("visitorid").agg(
    count("*").alias("total_events")
)

engagement = visitor_events.withColumn(
    "engagement_group",
    when(col("total_events") == 1, "1 event")
    .when(col("total_events") <= 5, "2-5 events")
    .when(col("total_events") <= 10, "6-10 events")
    .when(col("total_events") <= 20, "11-20 events")
    .otherwise("More than 20")
)

engagement_counts = (
    engagement.groupBy("engagement_group")
    .count()
    .toPandas()
)

group_order = [
    "1 event", "2-5 events", "6-10 events",
    "11-20 events", "More than 20"
]

engagement_counts["order"] = engagement_counts[
    "engagement_group"
].map({name: i for i, name in enumerate(group_order)})

engagement_counts = engagement_counts.sort_values("order")

plt.figure(figsize=(9, 5))
plt.bar(
    engagement_counts["engagement_group"],
    engagement_counts["count"]
)
plt.title("Visitor Engagement Distribution")
plt.xlabel("Number of Events per Visitor")
plt.ylabel("Number of Visitors")
plt.grid(axis="y", alpha=0.3)

save_chart("02_visitor_engagement.png")


# --------------------------------------------------
# CHART 3: HOURLY SHOPPING ACTIVITY
# --------------------------------------------------

print("\nChart 3: Hourly activity")

hourly_df = df.withColumn(
    "event_hour",
    hour(from_unixtime(col("timestamp") / 1000))
)

hourly_counts = (
    hourly_df.groupBy("event_hour")
    .count()
    .orderBy("event_hour")
    .toPandas()
)

plt.figure(figsize=(10, 5))
plt.plot(
    hourly_counts["event_hour"],
    hourly_counts["count"],
    marker="o"
)
plt.title("E-commerce Activity by Hour")
plt.xlabel("Hour of Day (Spark session timezone)")
plt.ylabel("Number of Events")
plt.xticks(range(0, 24))
plt.grid(alpha=0.3)

save_chart("03_hourly_activity.png")


# --------------------------------------------------
# CHART 4: TOP 10 VIEWED PRODUCTS
# --------------------------------------------------

print("\nChart 4: Most viewed products")

top_viewed = (
    df.filter(col("event") == "view")
    .groupBy("itemid")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
    .toPandas()
)

top_viewed["itemid"] = top_viewed["itemid"].astype(str)

plt.figure(figsize=(10, 6))
plt.barh(top_viewed["itemid"], top_viewed["count"])
plt.gca().invert_yaxis()
plt.title("Top 10 Most Viewed Products")
plt.xlabel("Number of Views")
plt.ylabel("Product ID")
plt.grid(axis="x", alpha=0.3)

save_chart("04_top_viewed_products.png")


# --------------------------------------------------
# CHART 5: TOP 10 TRANSACTED PRODUCTS
# --------------------------------------------------

print("\nChart 5: Products in transaction events")

top_transacted = (
    df.filter(
        (col("event") == "transaction")
        & col("itemid").isNotNull()
    )
    .groupBy("itemid")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
    .toPandas()
)

top_transacted["itemid"] = top_transacted["itemid"].astype(str)

plt.figure(figsize=(10, 6))
plt.barh(top_transacted["itemid"], top_transacted["count"])
plt.gca().invert_yaxis()
plt.title("Top 10 Products in Transaction Events")
plt.xlabel("Number of Transaction Events")
plt.ylabel("Product ID")
plt.grid(axis="x", alpha=0.3)

save_chart("05_top_transacted_products.png")


# --------------------------------------------------
# CHART 6: MACHINE LEARNING MODEL COMPARISON
# --------------------------------------------------

print("\nChart 6: Model comparison")

models = ["Logistic Regression", "Random Forest"]

metrics = {
    "Accuracy": [0.9919, 0.9967],
    "Precision": [0.5051, 0.9240],
    "Recall": [0.0432, 0.6478],
    "F1-score": [0.0796, 0.7616]
}

x = list(range(len(models)))
width = 0.18

plt.figure(figsize=(11, 6))

for i, (metric_name, values) in enumerate(metrics.items()):
    positions = [
        position + (i - 1.5) * width for position in x
    ]

    bars = plt.bar(
        positions,
        values,
        width,
        label=metric_name
    )

    plt.bar_label(
        bars,
        labels=[f"{v:.2f}" for v in values],
        fontsize=8,
        padding=2
    )

plt.title("RetailIQ: Machine Learning Model Comparison")
plt.xlabel("Classification Model")
plt.ylabel("Score")
plt.xticks(x, models)
plt.ylim(0, 1.15)
plt.legend()
plt.grid(axis="y", alpha=0.3)

save_chart("06_model_comparison.png")


# --------------------------------------------------
# FINISH
# --------------------------------------------------

print("\nAll six visualizations generated successfully.")
print("Output folder:", OUTPUT_DIR)

spark.stop()
