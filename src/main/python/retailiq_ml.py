
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("RetailIQ_PySpark_ML")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

dataset_path = "file:///home/satwiqreddy/SparkProject/data/events.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(dataset_path)
)

print("\n========== TASK 3.1: DATA LOADING ==========")
print("Total records:", df.count())

# TASK 3.2: FEATURE ENGINEERING
print("\n========== TASK 3.2: FEATURE PREPARATION ==========")

# Aggregate event counts for each visitor
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

# Label visitors: 1 = transaction recorded, 0 = no transaction recorded
visitor_features = visitor_features.withColumn(
    "label",
    when(col("transaction_count") > 0, 1.0).otherwise(0.0)
)

# Keep features and label; transaction_count is not used as a feature
model_data = visitor_features.select(
    "visitorid",
    "total_events",
    "view_count",
    "cart_count",
    "label"
)

print("Number of visitors:", model_data.count())
print("Feature columns: total_events, view_count, cart_count")
print("Label: 1 = transaction recorded, 0 = no transaction recorded")

print("\nSample prepared data:")
model_data.show(10, truncate=False)

print("\nLabel distribution:")
model_data.groupBy("label").count().orderBy("label").show()


# TASK 3.3: TRAIN-TEST SPLIT
print("\n========== TASK 3.3: TRAIN-TEST SPLIT ==========")

train_data, test_data = model_data.randomSplit(
    [0.8, 0.2],
    seed=42
)

print("Training records:", train_data.count())
print("Testing records:", test_data.count())

print("\nTraining label distribution:")
train_data.groupBy("label").count().orderBy("label").show()

print("\nTesting label distribution:")
test_data.groupBy("label").count().orderBy("label").show()


# TASK 3.4: TRAIN A LOGISTIC REGRESSION MODEL
print("\n========== TASK 3.4: MODEL TRAINING ==========")

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Input features
feature_columns = [
    "total_events",
    "view_count",
    "cart_count"
]

# Convert feature columns into a single vector
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="raw_features"
)

# Scale the features
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=False
)

# Create the Logistic Regression classifier
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    regParam=0.01
)

# Assemble preprocessing and model into a pipeline
pipeline = Pipeline(stages=[assembler, scaler, lr])

# Train only on the training dataset
model = pipeline.fit(train_data)

print("Logistic Regression model trained successfully.")

# Generate predictions on unseen test data
predictions = model.transform(test_data)

print("\nSample predictions:")
predictions.select(
    "total_events",
    "view_count",
    "cart_count",
    "label",
    "prediction",
    "probability"
).show(10, truncate=False)

# TASK 3.5: MODEL EVALUATION
print("\n========== TASK 3.5: MODEL EVALUATION ==========")

from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import col

# Convert actual and predicted labels into the format
# required by Spark's evaluation metrics
prediction_and_labels = predictions.select(
    col("prediction").cast("double"),
    col("label").cast("double")
)

# Calculate evaluation metrics
metrics = MulticlassMetrics(
    prediction_and_labels.rdd.map(
        lambda row: (row["prediction"], row["label"])
    )
)

accuracy = metrics.accuracy
precision = metrics.precision(1.0)
recall = metrics.recall(1.0)
f1_score = metrics.fMeasure(1.0)

print(f"Accuracy:  {accuracy:.4f} ({accuracy * 100:.2f}%)")
print(f"Precision: {precision:.4f}")
print(f"Recall:    {recall:.4f}")
print(f"F1-score:  {f1_score:.4f}")

print("\nConfusion Matrix:")
print(metrics.confusionMatrix().toArray())



# TASK 3.6: RANDOM FOREST CLASSIFIER
print("\n========== TASK 3.6: RANDOM FOREST MODEL ==========")

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.mllib.evaluation import MulticlassMetrics

# Prepare features
rf_assembler = VectorAssembler(
    inputCols=["total_events", "view_count", "cart_count"],
    outputCol="features"
)

# Create Random Forest classifier
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,
    maxDepth=8,
    seed=42
)

# Build and train the pipeline
rf_pipeline = Pipeline(stages=[rf_assembler, rf])

rf_model = rf_pipeline.fit(train_data)

print("Random Forest model trained successfully.")

# Generate predictions on the same test dataset
rf_predictions = rf_model.transform(test_data)

print("\nSample Random Forest predictions:")
rf_predictions.select(
    "total_events",
    "view_count",
    "cart_count",
    "label",
    "prediction"
).show(10, truncate=False)

# Evaluate Random Forest
rf_prediction_labels = rf_predictions.select(
    col("prediction").cast("double"),
    col("label").cast("double")
)

rf_metrics = MulticlassMetrics(
    rf_prediction_labels.rdd.map(
        lambda row: (row["prediction"], row["label"])
    )
)

rf_accuracy = rf_metrics.accuracy
rf_precision = rf_metrics.precision(1.0)
rf_recall = rf_metrics.recall(1.0)
rf_f1 = rf_metrics.fMeasure(1.0)

print("\n========== RANDOM FOREST EVALUATION ==========")
print(f"Accuracy:  {rf_accuracy:.4f} ({rf_accuracy * 100:.2f}%)")
print(f"Precision: {rf_precision:.4f}")
print(f"Recall:    {rf_recall:.4f}")
print(f"F1-score:  {rf_f1:.4f}")

print("\nRandom Forest Confusion Matrix:")
print(rf_metrics.confusionMatrix().toArray())

# Compare the two models
print("\n========== MODEL COMPARISON ==========")
print("Model                  Accuracy   Precision   Recall     F1-score")
print(
    f"Logistic Regression   "
    f"{accuracy:.4f}     {precision:.4f}      "
    f"{recall:.4f}    {f1_score:.4f}"
)
print(
    f"Random Forest         "
    f"{rf_accuracy:.4f}     {rf_precision:.4f}      "
    f"{rf_recall:.4f}    {rf_f1:.4f}"
)


spark.stop()
