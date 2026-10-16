
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, max as spark_max,
    min as spark_min, when, lit, log1p,
    sum as spark_sum
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# --------------------------------------------------
# 1. Spark setup
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("EnhancedPurchasePrediction")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BASE = "/home/satwiqreddy/SparkProject"
DATA = f"file://{BASE}/data/events.csv"
EVIDENCE = f"{BASE}/review_evidence"
MODEL_PATH = f"{BASE}/output/enhanced_purchase_model"

# --------------------------------------------------
# 2. Load and clean dataset
# --------------------------------------------------
events = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA)
    .select("timestamp", "visitorid", "event", "itemid")
    .dropna(subset=["timestamp", "visitorid", "event", "itemid"])
)

# July 1, 2015: historical cutoff
cutoff = 1435708800000

past = events.filter(col("timestamp") < cutoff)

future = events.filter(
    (col("timestamp") >= cutoff) &
    (col("event") == "transaction")
)

# --------------------------------------------------
# 3. Build visitor-level features from past events
# --------------------------------------------------
features = past.groupBy("visitorid").agg(
    count("*").alias("total_events"),
    count(when(col("event") == "view", True)).alias("views"),
    count(when(col("event") == "addtocart", True)).alias("carts"),
    countDistinct("itemid").alias("unique_items"),
    countDistinct(
        when(col("event") == "view", col("itemid"))
    ).alias("unique_viewed"),
    countDistinct(
        when(col("event") == "addtocart", col("itemid"))
    ).alias("unique_carted"),
    spark_max("timestamp").alias("last_activity"),
    spark_min("timestamp").alias("first_activity")
)

# Label = 1 if the visitor transacts in the future period
buyers = (
    future.select("visitorid")
    .distinct()
    .withColumn("label", lit(1.0))
)

cutoff_lit = lit(cutoff)

# --------------------------------------------------
# 4. Engineer features
# --------------------------------------------------
data = (
    features.join(buyers, "visitorid", "left")
    .fillna({"label": 0.0})
    .filter((col("views") + col("carts")) > 0)

    # Log transforms reduce the influence of extreme counts
    .withColumn("log_views", log1p(col("views")))
    .withColumn("log_carts", log1p(col("carts")))
    .withColumn("log_total", log1p(col("total_events")))
    .withColumn("log_unique_items", log1p(col("unique_items")))
    .withColumn("log_unique_viewed", log1p(col("unique_viewed")))
    .withColumn("log_unique_carted", log1p(col("unique_carted")))

    # Time-related features
    .withColumn(
        "days_active",
        (col("last_activity") - col("first_activity")) /
        lit(86400000.0)
    )
    .withColumn("log_days_active", log1p(col("days_active")))
    .withColumn(
        "hours_since_activity",
        (cutoff_lit - col("last_activity")) / lit(3600000.0)
    )
    .withColumn("log_recency", log1p(col("hours_since_activity")))

    # Existing cart signal
    .withColumn(
        "has_cart",
        when(col("carts") > 0, 1.0).otherwise(0.0)
    )

    # NEW FEATURE 1: cart events relative to views
    .withColumn(
        "cart_view_ratio",
        col("carts") / (col("views") + lit(1.0))
    )

    # NEW FEATURE 2: total activity relative to active duration
    .withColumn(
        "events_per_active_day",
        col("total_events") / (col("days_active") + lit(1.0))
    )

    .select(
        "log_views",
        "log_carts",
        "log_total",
        "log_unique_items",
        "log_unique_viewed",
        "log_unique_carted",
        "log_days_active",
        "log_recency",
        "has_cart",
        "cart_view_ratio",
        "events_per_active_day",
        "label"
    )
    .cache()
)

print("Total visitor records:", data.count())
print("Buyer records:", data.filter(col("label") == 1.0).count())

# --------------------------------------------------
# 5. Train / validation / test split
# --------------------------------------------------
train, validation, test = data.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)

positive = train.filter(col("label") == 1.0).count()
negative = train.filter(col("label") == 0.0).count()

print("Training positives:", positive)
print("Training negatives:", negative)
print("Validation rows:", validation.count())
print("Test rows:", test.count())

if positive == 0 or negative == 0:
    raise ValueError("Training split must contain both classes.")

# Undersample negative examples in TRAINING only.
# Validation and test retain their original class distribution.
# Experiment: retain approximately 3 negative examples
# for every positive example in the training set.
NEGATIVE_TO_POSITIVE_RATIO = 3.0

fraction = min(
    1.0,
    (NEGATIVE_TO_POSITIVE_RATIO * positive) / negative
)

balanced_train = train.sampleBy(
    "label",
    fractions={0.0: fraction, 1.0: 1.0},
    seed=42
).cache()

print("Balanced training rows:", balanced_train.count())

# --------------------------------------------------
# 6. Assemble the feature vector
# --------------------------------------------------
input_cols = [
    "log_views",
    "log_carts",
    "log_total",
    "log_unique_items",
    "log_unique_viewed",
    "log_unique_carted",
    "log_days_active",
    "log_recency",
    "has_cart",
    "cart_view_ratio",
    "events_per_active_day"
]

assembler = VectorAssembler(
    inputCols=input_cols,
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

# --------------------------------------------------
# 7. Define three models
# --------------------------------------------------
models = {
    "Enhanced Logistic Regression": LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.0
    ),

    "Enhanced Random Forest": RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=8,
        seed=42
    ),

    "Enhanced GBT": GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=50,
        maxDepth=5,
        stepSize=0.05,
        seed=42
    )
}

# Test a range of classification thresholds.
thresholds = [i / 100.0 for i in range(5, 96, 1)]

results = []
best_lr_f1 = -1.0
best_lr_threshold = 0.5
best_lr_result = None

# --------------------------------------------------
# 8. Train, tune, and evaluate each model
# --------------------------------------------------
for name, classifier in models.items():
    print("\n" + "=" * 65)
    print("Training:", name)

    pipeline = Pipeline(stages=[assembler, scaler, classifier])
    fitted = pipeline.fit(balanced_train)

    # Generate validation probabilities.
    validation_scores = (
        fitted.transform(validation)
        .withColumn(
            "positive_probability",
            vector_to_array(col("probability"))[1]
        )
        .cache()
    )

    # Calculate all threshold confusion counts in one Spark action.
    expressions = []

    for i, threshold in enumerate(thresholds):
        predicted_positive = col("positive_probability") >= lit(threshold)
        actual_positive = col("label") == 1.0
        actual_negative = col("label") == 0.0

        expressions.extend([
            spark_sum(
                when(actual_positive & predicted_positive, 1)
                .otherwise(0)
            ).alias(f"tp_{i}"),

            spark_sum(
                when(actual_negative & predicted_positive, 1)
                .otherwise(0)
            ).alias(f"fp_{i}"),

            spark_sum(
                when(actual_positive & ~predicted_positive, 1)
                .otherwise(0)
            ).alias(f"fn_{i}")
        ])

    validation_counts = validation_scores.agg(*expressions).first()

    best_threshold = 0.5
    best_f1 = -1.0

    for i, threshold in enumerate(thresholds):
        tp = int(validation_counts[f"tp_{i}"] or 0)
        fp = int(validation_counts[f"fp_{i}"] or 0)
        fn = int(validation_counts[f"fn_{i}"] or 0)

        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0

        f1 = (
            2 * precision * recall / (precision + recall)
            if precision + recall else 0.0
        )

        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold

    print(f"Best validation threshold: {best_threshold:.2f}")
    print(f"Best validation F1: {best_f1:.4f}")

    # Evaluate once on the untouched test set.
    test_scores = (
        fitted.transform(test)
        .withColumn(
            "positive_probability",
            vector_to_array(col("probability"))[1]
        )
    )

    predictions = test_scores.withColumn(
        "prediction",
        when(
            col("positive_probability") >= lit(best_threshold),
            1.0
        ).otherwise(0.0)
    ).cache()

    # Calculate test confusion matrix.
    counts = predictions.agg(
        spark_sum(
            when(
                (col("label") == 1.0) & (col("prediction") == 1.0), 1
            ).otherwise(0)
        ).alias("tp"),

        spark_sum(
            when(
                (col("label") == 0.0) & (col("prediction") == 1.0), 1
            ).otherwise(0)
        ).alias("fp"),

        spark_sum(
            when(
                (col("label") == 0.0) & (col("prediction") == 0.0), 1
            ).otherwise(0)
        ).alias("tn"),

        spark_sum(
            when(
                (col("label") == 1.0) & (col("prediction") == 0.0), 1
            ).otherwise(0)
        ).alias("fn")
    ).first()

    tp = int(counts["tp"] or 0)
    fp = int(counts["fp"] or 0)
    tn = int(counts["tn"] or 0)
    fn = int(counts["fn"] or 0)

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0.0
    )

    total = tp + fp + tn + fn
    accuracy = (tp + tn) / total if total else 0.0

    # PR-AUC is calculated from model scores, not thresholded predictions.
    pr_auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    ).evaluate(fitted.transform(test))

    result = (
        f"{name} | threshold={best_threshold:.2f} | "
        f"accuracy={accuracy:.4f} | precision={precision:.4f} | "
        f"recall={recall:.4f} | F1={f1:.4f} | "
        f"PR-AUC={pr_auc:.4f} | "
        f"TP={tp} FP={fp} TN={tn} FN={fn}"
    )

    print(result)
    results.append(result)

    # Save the Logistic Regression model for the existing dashboard.
    # Keep this model path so website/app.py can load it.
    if name == "Enhanced Logistic Regression":
        fitted.write().overwrite().save(MODEL_PATH)

        with open(
            f"{BASE}/output/enhanced_purchase_threshold.txt", "w"
        ) as threshold_file:
            threshold_file.write(f"{best_threshold:.2f}\n")

        with open(
            f"{BASE}/output/enhanced_purchase_model_metrics.txt", "w"
        ) as metrics_file:
            metrics_file.write(result + "\n")

        best_lr_f1 = f1
        best_lr_threshold = best_threshold
        best_lr_result = result

        print("Saved Logistic Regression model:", MODEL_PATH)
        print("Saved selected threshold:", best_lr_threshold)

    predictions.unpersist()
    validation_scores.unpersist()

# --------------------------------------------------
# 9. Save experiment results
# --------------------------------------------------
with open(f"{EVIDENCE}/enhanced_model_comparison.txt", "w") as f:
    f.write("\n".join(results) + "\n")

print("\n" + "=" * 65)
print("MODEL COMPARISON COMPLETE")
print("Results saved to:", f"{EVIDENCE}/enhanced_model_comparison.txt")
print("Dashboard model:", MODEL_PATH)
print("Logistic Regression threshold:", best_lr_threshold)
print("Logistic Regression test F1:", f"{best_lr_f1:.4f}")

balanced_train.unpersist()
data.unpersist()
spark.stop()
