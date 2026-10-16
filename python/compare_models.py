
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, max as spark_max,
    when, lit, log1p
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (
    SparkSession.builder
    .appName("ComparePurchaseModels")
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
    count(when(col("event") == "view", True)).alias("views"),
    count(when(col("event") == "addtocart", True)).alias("carts"),
    countDistinct("itemid").alias("items"),
    spark_max("timestamp").alias("last_activity")
)

buyers = future.select("visitorid").distinct().withColumn("label", lit(1.0))

data = (
    features.join(buyers, "visitorid", "left")
    .fillna({"label": 0.0})
    .filter((col("views") + col("carts")) > 0)
    .withColumn("log_views", log1p(col("views")))
    .withColumn("log_carts", log1p(col("carts")))
    .withColumn("log_items", log1p(col("items")))
    .select("log_views", "log_carts", "log_items", "label")
    .cache()
)

train, validation, test = data.randomSplit([0.70, 0.15, 0.15], seed=42)

pos = train.filter(col("label") == 1.0).count()
neg = train.filter(col("label") == 0.0).count()
if pos == 0 or neg == 0:
    raise ValueError("Training data must contain both classes.")

negative_fraction = min(1.0, (10.0 * pos) / neg)
balanced_train = train.sampleBy(
    "label", fractions={0.0: negative_fraction, 1.0: 1.0}, seed=42
).cache()

assembler = VectorAssembler(
    inputCols=["log_views", "log_carts", "log_items"],
    outputCol="raw_features"
)
scaler = StandardScaler(
    inputCol="raw_features", outputCol="features",
    withMean=True, withStd=True
)

models = {
    "Logistic Regression": LogisticRegression(
        featuresCol="features", labelCol="label",
        maxIter=100, regParam=0.01
    ),
    "Random Forest": RandomForestClassifier(
        featuresCol="features", labelCol="label",
        numTrees=80, maxDepth=8, seed=42,
        subsamplingRate=0.8
    )
}

results = []

for name, classifier in models.items():
    print(f"\n===== TRAINING {name} =====")
    pipeline = Pipeline(stages=[assembler, scaler, classifier])
    fitted = pipeline.fit(balanced_train)

    val_scores = fitted.transform(validation).withColumn(
        "positive_probability", vector_to_array(col("probability"))[1]
    ).cache()

    best_threshold = 0.5
    best_f1 = -1.0

    for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        pred = val_scores.withColumn(
            "prediction",
            when(col("positive_probability") >= threshold, 1.0).otherwise(0.0)
        )
        tp = pred.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
        fp = pred.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
        fn = pred.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold

    print(f"Best validation threshold: {best_threshold:.1f}")
    print(f"Best validation F1: {best_f1:.4f}")

    # Test set is evaluated only after choosing the threshold on validation
    test_scores = fitted.transform(test).withColumn(
        "positive_probability", vector_to_array(col("probability"))[1]
    )
    pred = test_scores.withColumn(
        "prediction",
        when(col("positive_probability") >= best_threshold, 1.0).otherwise(0.0)
    ).cache()

    tp = pred.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
    fp = pred.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
    tn = pred.filter((col("label") == 0.0) & (col("prediction") == 0.0)).count()
    fn = pred.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn)

    pr_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    ).evaluate(fitted.transform(test))

    result = {
        "model": name,
        "threshold": best_threshold,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "pr_auc": pr_auc,
        "tp": tp, "fp": fp, "tn": tn, "fn": fn
    }
    results.append(result)

    print(
        f"TEST | Accuracy={accuracy:.4f} Precision={precision:.4f} "
        f"Recall={recall:.4f} F1={f1:.4f} PR-AUC={pr_auc:.4f}"
    )
    print(f"TP={tp}, FP={fp}, TN={tn}, FN={fn}")

    pred.unpersist()
    val_scores.unpersist()

print("\n===== FINAL MODEL COMPARISON =====")
for r in results:
    print(
        f"{r['model']} | threshold={r['threshold']:.1f} | "
        f"accuracy={r['accuracy']:.4f} | precision={r['precision']:.4f} | "
        f"recall={r['recall']:.4f} | F1={r['f1']:.4f} | PR-AUC={r['pr_auc']:.4f}"
    )

with open("/home/satwiqreddy/SparkProject/review_evidence/model_comparison.txt", "w") as f:
    for r in results:
        f.write(
            f"{r['model']} | threshold={r['threshold']:.1f} | "
            f"accuracy={r['accuracy']:.4f} | precision={r['precision']:.4f} | "
            f"recall={r['recall']:.4f} | F1={r['f1']:.4f} | PR-AUC={r['pr_auc']:.4f}\n"
        )

balanced_train.unpersist()
data.unpersist()
spark.stop()
