
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, countDistinct, when,
    max as spark_max, min as spark_min, log1p
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

BASE = "/home/satwiqreddy/SparkProject"
DATA = f"file://{BASE}/data/events.csv"

spark = (
    SparkSession.builder
    .appName("RetailIQTemporalPurchaseModel")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(DATA)
    .select("timestamp", "visitorid", "event", "itemid")
    .dropna(subset=["timestamp", "visitorid", "event", "itemid"])
    .cache()
)

# All times are UTC epoch milliseconds.
def epoch_ms(date_text):
    dt = datetime.strptime(date_text, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    return int(dt.timestamp() * 1000)

# Each snapshot has a 30-day lookback and a 7-day label window.
snapshots = [
    ("2015-06-01", "train"),
    ("2015-06-15", "train"),
    ("2015-07-01", "train"),
    ("2015-07-15", "validation"),
    ("2015-08-01", "validation"),
    ("2015-08-15", "test"),
    ("2015-09-01", "test"),
]

LOOKBACK_MS = 30 * 24 * 60 * 60 * 1000
HORIZON_MS = 7 * 24 * 60 * 60 * 1000

snapshots_data = []

for date_text, split_name in snapshots:
    cutoff = epoch_ms(date_text)
    lookback_start = cutoff - LOOKBACK_MS

    # Only events in the 30-day historical window become features.
    past = events.filter(
        (col("timestamp") >= lookback_start)
        & (col("timestamp") < cutoff)
    )

    # Only transactions in the next 7 days become labels.
    future_buyers = (
        events.filter(
            (col("timestamp") >= cutoff)
            & (col("timestamp") < cutoff + HORIZON_MS)
            & (col("event") == "transaction")
        )
        .select("visitorid")
        .distinct()
        .withColumn("label", lit(1.0))
    )

    features = past.groupBy("visitorid").agg(
        count("*").alias("total_events"),
        count(when(col("event") == "view", True)).alias("views"),
        count(when(col("event") == "addtocart", True)).alias("carts"),
        count(
            when(col("event") == "transaction", True)
        ).alias("previous_purchases"),
        countDistinct("itemid").alias("unique_items"),
        countDistinct(
            when(col("event") == "view", col("itemid"))
        ).alias("unique_viewed"),
        countDistinct(
            when(col("event") == "addtocart", col("itemid"))
        ).alias("unique_carted"),
        spark_max("timestamp").alias("last_activity"),
        spark_min("timestamp").alias("first_activity"),
        spark_max(
            when(col("event") == "addtocart", col("timestamp"))
        ).alias("last_cart_activity"),
    )

    frame = (
        features.join(future_buyers, "visitorid", "left")
        .fillna({"label": 0.0})
        .filter((col("views") + col("carts")) > 0)
        .withColumn(
            "days_active",
            (col("last_activity") - col("first_activity"))
            / lit(86400000.0)
        )
        .withColumn(
            "hours_since_activity",
            (lit(cutoff) - col("last_activity"))
            / lit(3600000.0)
        )
        .withColumn(
            "hours_since_last_cart",
            when(
                col("last_cart_activity").isNotNull(),
                (lit(cutoff) - col("last_cart_activity"))
                / lit(3600000.0)
            ).otherwise(lit(0.0))
        )
        .withColumn("log_views", log1p(col("views")))
        .withColumn("log_carts", log1p(col("carts")))
        .withColumn("log_total", log1p(col("total_events")))
        .withColumn("log_unique_items", log1p(col("unique_items")))
        .withColumn("log_unique_viewed", log1p(col("unique_viewed")))
        .withColumn("log_unique_carted", log1p(col("unique_carted")))
        .withColumn("log_previous_purchases", log1p(col("previous_purchases")))
        .withColumn("log_days_active", log1p(col("days_active")))
        .withColumn("log_recency", log1p(col("hours_since_activity")))
        .withColumn(
            "log_hours_since_last_cart",
            log1p(col("hours_since_last_cart"))
        )
        .withColumn(
            "cart_view_ratio",
            col("carts") / (col("views") + lit(1.0))
        )
        .withColumn(
            "views_per_active_day",
            col("views") / (col("days_active") + lit(1.0))
        )
        .withColumn(
            "has_cart",
            when(col("carts") > 0, 1.0).otherwise(0.0)
        )
        .withColumn("snapshot_date", lit(date_text))
        .withColumn("split", lit(split_name))
        .select(
            "visitorid", "snapshot_date", "split", "label",
            "log_views", "log_carts", "log_total",
            "log_unique_items", "log_unique_viewed",
            "log_unique_carted", "log_previous_purchases",
            "log_days_active", "log_recency",
            "log_hours_since_last_cart", "cart_view_ratio",
            "views_per_active_day", "has_cart"
        )
        .cache()
    )

    total = frame.count()
    buyers = frame.filter(col("label") == 1.0).count()

    print(
        f"{date_text} [{split_name}]: "
        f"rows={total:,}, buyers={buyers:,}"
    )

    snapshots_data.append(frame)

data = snapshots_data[0]
for frame in snapshots_data[1:]:
    data = data.unionByName(frame)

print("\nRows by split:")
data.groupBy("split").count().show()

print("Labels by split:")
data.groupBy("split", "label").count().orderBy("split", "label").show()

feature_cols = [
    "log_views",
    "log_carts",
    "log_total",
    "log_unique_items",
    "log_unique_viewed",
    "log_unique_carted",
    "log_previous_purchases",
    "log_days_active",
    "log_recency",
    "log_hours_since_last_cart",
    "cart_view_ratio",
    "views_per_active_day",
    "has_cart",
]

train = data.filter(col("split") == "train")
validation = data.filter(col("split") == "validation")
test = data.filter(col("split") == "test")

train_pos = train.filter(col("label") == 1.0).count()
train_neg = train.filter(col("label") == 0.0).count()
val_pos = validation.filter(col("label") == 1.0).count()
test_pos = test.filter(col("label") == 1.0).count()

if min(train_pos, val_pos, test_pos) == 0:
    raise ValueError(
        "At least one split has no buyers. Review the snapshot counts "
        "before training."
    )

print(
    f"Training: {train_pos} buyers / {train_neg} non-buyers; "
    f"validation buyers={val_pos}; test buyers={test_pos}"
)

# Undersample negatives in training only.
ratio = 10.0
negative_fraction = min(1.0, ratio * train_pos / train_neg)

balanced_train = train.sampleBy(
    "label",
    fractions={0.0: negative_fraction, 1.0: 1.0},
    seed=42
).cache()

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

classifier = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01
)

pipeline = Pipeline(stages=[assembler, scaler, classifier])
model = pipeline.fit(balanced_train)

# Choose the threshold using validation only.
val_scores = model.transform(validation).withColumn(
    "score", vector_to_array(col("probability"))[1]
).cache()

best_threshold = 0.5
best_f1 = -1.0

for threshold_pct in range(5, 96):
    threshold = threshold_pct / 100.0

    pred = val_scores.withColumn(
        "prediction",
        when(col("score") >= lit(threshold), 1.0).otherwise(0.0)
    )

    counts = pred.agg(
        count(when((col("label") == 1.0) & (col("prediction") == 1.0), True)).alias("tp"),
        count(when((col("label") == 0.0) & (col("prediction") == 1.0), True)).alias("fp"),
        count(when((col("label") == 1.0) & (col("prediction") == 0.0), True)).alias("fn"),
    ).first()

    tp, fp, fn = counts["tp"], counts["fp"], counts["fn"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0.0
    )

    if f1 > best_f1:
        best_f1, best_threshold = f1, threshold

# Final evaluation on later snapshots.
test_scores = model.transform(test).withColumn(
    "score", vector_to_array(col("probability"))[1]
)

pred = test_scores.withColumn(
    "prediction",
    when(col("score") >= lit(best_threshold), 1.0).otherwise(0.0)
).cache()

counts = pred.agg(
    count(when((col("label") == 1.0) & (col("prediction") == 1.0), True)).alias("tp"),
    count(when((col("label") == 0.0) & (col("prediction") == 1.0), True)).alias("fp"),
    count(when((col("label") == 0.0) & (col("prediction") == 0.0), True)).alias("tn"),
    count(when((col("label") == 1.0) & (col("prediction") == 0.0), True)).alias("fn"),
).first()

tp, fp, tn, fn = counts["tp"], counts["fp"], counts["tn"], counts["fn"]
precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
total = tp + fp + tn + fn
accuracy = (tp + tn) / total if total else 0.0

pr_auc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
).evaluate(model.transform(test))

result = (
    f"Temporal Logistic Regression | threshold={best_threshold:.2f} | "
    f"accuracy={accuracy:.4f} | precision={precision:.4f} | "
    f"recall={recall:.4f} | F1={f1:.4f} | PR-AUC={pr_auc:.4f} | "
    f"TP={tp} FP={fp} TN={tn} FN={fn}"
)

print("\n", result)
print("Selected threshold from validation:", best_threshold)

# Save to separate paths for now. Do not overwrite the current website model yet.
model_path = f"{BASE}/output/temporal_purchase_model"
model.write().overwrite().save(model_path)

with open(f"{BASE}/output/temporal_purchase_threshold.txt", "w") as f:
    f.write(f"{best_threshold:.2f}\n")

with open(f"{BASE}/output/temporal_purchase_metrics.txt", "w") as f:
    f.write(result + "\n")

print("Saved experimental model to:", model_path)
print("Saved experimental metrics and threshold.")

pred.unpersist()
val_scores.unpersist()
balanced_train.unpersist()
data.unpersist()
for frame in snapshots_data:
    frame.unpersist()
events.unpersist()
spark.stop()
