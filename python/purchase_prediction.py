
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, max as spark_max,
    when, lit, log1p
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (
    SparkSession.builder
    .appName("ImprovedEcommercePurchasePrediction")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "file:///home/satwiqreddy/SparkProject/data/events.csv"
OUTPUT_PATH = "/home/satwiqreddy/SparkProject/output"
EVIDENCE_PATH = "/home/satwiqreddy/SparkProject/review_evidence"

# 1. Load dataset
events = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
    .select("timestamp", "visitorid", "event", "itemid")
    .dropna(subset=["timestamp", "visitorid", "event", "itemid"])
)

print("Total events:", events.count())

# 2. Define prediction target using a chronological cutoff
cutoff = 1435708800000

past_events = events.filter(col("timestamp") < cutoff)
future_transactions = events.filter(
    (col("timestamp") >= cutoff) &
    (col("event") == "transaction")
)

# 3. Create visitor features from past activity only
features = past_events.groupBy("visitorid").agg(
    count(when(col("event") == "view", True)).alias("view_count"),
    count(when(col("event") == "addtocart", True)).alias("cart_count"),
    countDistinct("itemid").alias("distinct_items"),
    spark_max("timestamp").alias("last_activity")
)

buyers = (
    future_transactions.select("visitorid").distinct()
    .withColumn("label", lit(1.0))
)

dataset = (
    features.join(buyers, "visitorid", "left")
    .fillna({"label": 0.0})
    .filter((col("view_count") + col("cart_count")) > 0)
    .withColumn("log_views", log1p(col("view_count")))
    .withColumn("log_carts", log1p(col("cart_count")))
    .withColumn("log_items", log1p(col("distinct_items")))
    .select("log_views", "log_carts", "log_items", "label")
    .cache()
)

print("\n===== DATASET =====")
dataset.groupBy("label").count().show()
print("Total visitors:", dataset.count())

# 4. Split by visitor row into train, validation and test
train, validation, test = dataset.randomSplit(
    [0.70, 0.15, 0.15], seed=42
)

train = train.cache()
validation = validation.cache()
test = test.cache()

positive_count = train.filter(col("label") == 1.0).count()
negative_count = train.filter(col("label") == 0.0).count()

print("Training positives:", positive_count)
print("Training negatives:", negative_count)
print("Validation rows:", validation.count())
print("Test rows:", test.count())

if positive_count == 0 or negative_count == 0:
    raise ValueError("Training split must contain both classes.")

# 5. Undersample negatives in TRAINING ONLY, to a 10:1 ratio
negative_fraction = min(
    1.0, (10.0 * positive_count) / negative_count
)

balanced_train = train.sampleBy(
    "label",
    fractions={0.0: negative_fraction, 1.0: 1.0},
    seed=42
).cache()

print("\n===== TRAINING AFTER UNDERSAMPLING =====")
balanced_train.groupBy("label").count().show()

# 6. Build the pipeline
assembler = VectorAssembler(
    inputCols=["log_views", "log_carts", "log_items"],
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.0
)

pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(balanced_train)

# 7. Select threshold using VALIDATION data only
validation_scores = model.transform(validation).withColumn(
    "probability_positive",
    vector_to_array(col("probability"))[1]
).cache()

best_threshold = 0.5
best_f1 = -1.0

print("\n===== VALIDATION THRESHOLD SEARCH =====")

for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
    scored = validation_scores.withColumn(
        "prediction",
        when(col("probability_positive") >= threshold, 1.0)
        .otherwise(0.0)
    )

    tp = scored.filter(
        (col("label") == 1.0) & (col("prediction") == 1.0)
    ).count()
    fp = scored.filter(
        (col("label") == 0.0) & (col("prediction") == 1.0)
    ).count()
    fn = scored.filter(
        (col("label") == 1.0) & (col("prediction") == 0.0)
    ).count()

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0.0
    )

    print(
        f"Threshold={threshold:.1f} | "
        f"Precision={precision:.4f} "
        f"Recall={recall:.4f} F1={f1:.4f}"
    )

    if f1 > best_f1:
        best_f1 = f1
        best_threshold = threshold

print("Selected validation threshold:", best_threshold)
print("Best validation F1:", round(best_f1, 4))

# 8. Evaluate ONCE on the untouched test set
test_scores = model.transform(test).withColumn(
    "probability_positive",
    vector_to_array(col("probability"))[1]
)

predictions = test_scores.withColumn(
    "prediction",
    when(col("probability_positive") >= best_threshold, 1.0)
    .otherwise(0.0)
).cache()

tp = predictions.filter(
    (col("label") == 1.0) & (col("prediction") == 1.0)
).count()
fp = predictions.filter(
    (col("label") == 0.0) & (col("prediction") == 1.0)
).count()
tn = predictions.filter(
    (col("label") == 0.0) & (col("prediction") == 0.0)
).count()
fn = predictions.filter(
    (col("label") == 1.0) & (col("prediction") == 0.0)
).count()

precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
f1 = (
    2 * precision * recall / (precision + recall)
    if precision + recall else 0.0
)
accuracy = (tp + tn) / (tp + tn + fp + fn)

pr_auc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
).evaluate(model.transform(test))

print("\n===== FINAL TEST EVALUATION =====")
print(f"Selected threshold: {best_threshold:.1f}")
print(f"Accuracy : {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall   : {recall:.4f}")
print(f"F1-score : {f1:.4f}")
print(f"PR-AUC   : {pr_auc:.4f}")
print(f"True positives : {tp}")
print(f"False positives: {fp}")
print(f"True negatives : {tn}")
print(f"False negatives: {fn}")

print("\n===== CONFUSION MATRIX =====")
print("                 Predicted")
print("                 Buyer   Non-buyer")
print(f"Actual buyer     {tp:6d}   {fn:6d}")
print(f"Actual nonbuyer  {fp:6d}   {tn:6d}")

# 9. Save test predictions and trained model
(
    predictions.select(
        "label", "prediction", "probability_positive"
    )
    .coalesce(4)
    .write.mode("overwrite")
    .parquet(OUTPUT_PATH + "/predictions")
)

model.write().overwrite().save(OUTPUT_PATH + "/purchase_model")

with open(EVIDENCE_PATH + "/final_metrics.txt", "w") as f:
    f.write(f"Threshold: {best_threshold:.1f}\n")
    f.write(f"Accuracy: {accuracy:.4f}\n")
    f.write(f"Precision: {precision:.4f}\n")
    f.write(f"Recall: {recall:.4f}\n")
    f.write(f"F1-score: {f1:.4f}\n")
    f.write(f"PR-AUC: {pr_auc:.4f}\n")
    f.write(f"TP={tp}, FP={fp}, TN={tn}, FN={fn}\n")

print("\nSaved model, predictions and review metrics.")

predictions.unpersist()
validation_scores.unpersist()
balanced_train.unpersist()
train.unpersist()
validation.unpersist()
test.unpersist()
dataset.unpersist()
spark.stop()
