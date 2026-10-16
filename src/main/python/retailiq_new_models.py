
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline

# Start Spark
spark = (
    SparkSession.builder
    .appName("RetailIQ_New_Models_Only")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Load the original dataset
dataset_path = "file:///home/satwiqreddy/SparkProject/data/events.csv"
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(dataset_path)
)

# Feature engineering: same logic as the original script
visitor_features = (
    df.groupBy("visitorid")
    .agg(
        count("*").alias("total_events"),
        spark_sum(
            when(col("event") == "view", 1).otherwise(0)
        ).alias("view_count"),
        spark_sum(
            when(col("event") == "addtocart", 1).otherwise(0)
        ).alias("cart_count"),
        spark_sum(
            when(col("event") == "transaction", 1).otherwise(0)
        ).alias("transaction_count")
    )
)

visitor_features = visitor_features.withColumn(
    "label",
    when(col("transaction_count") > 0, 1.0).otherwise(0.0)
)

model_data = visitor_features.select(
    "visitorid", "total_events", "view_count",
    "cart_count", "label"
)

print("Number of visitors:", model_data.count())

# Same 80/20 split and seed
train_data, test_data = model_data.randomSplit(
    [0.8, 0.2], seed=42
)

print("Training records:", train_data.count())
print("Testing records:", test_data.count())

feature_columns = [
    "total_events", "view_count", "cart_count"
]

def evaluate_model(name, classifier):
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    pipeline = Pipeline(stages=[assembler, classifier])
    fitted_model = pipeline.fit(train_data)
    predictions = fitted_model.transform(test_data)

    prediction_labels = predictions.select(
        col("prediction").cast("double"),
        col("label").cast("double")
    )

    metrics = MulticlassMetrics(
        prediction_labels.rdd.map(
            lambda row: (row["prediction"], row["label"])
        )
    )

    accuracy = metrics.accuracy
    precision = metrics.precision(1.0)
    recall = metrics.recall(1.0)
    f1 = metrics.fMeasure(1.0)

    print(f"\n========== {name} ==========")
    print(f"Accuracy:  {accuracy:.4f} ({accuracy * 100:.2f}%)")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1-score:  {f1:.4f}")
    print("Confusion Matrix:")
    print(metrics.confusionMatrix().toArray())

    return {
        "name": name,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }

# Model 1: Decision Tree
dt_results = evaluate_model(
    "Decision Tree",
    DecisionTreeClassifier(
        featuresCol="features",
        labelCol="label",
        seed=42
    )
)

# Model 2: Gradient-Boosted Tree
gbt_results = evaluate_model(
    "Gradient-Boosted Tree",
    GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        maxDepth=5,
        seed=42
    )
)

print("\n========== NEW MODEL COMPARISON ==========")
print("Model                    Accuracy   Precision   Recall    F1-score")

for result in [dt_results, gbt_results]:
    print(
        f"{result['name']:<24}"
        f"{result['accuracy']:.4f}     "
        f"{result['precision']:.4f}      "
        f"{result['recall']:.4f}    "
        f"{result['f1']:.4f}"
    )

spark.stop()
